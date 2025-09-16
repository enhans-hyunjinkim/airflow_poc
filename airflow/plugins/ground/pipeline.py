# plugins/pipeline.py
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Optional, List

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models import XCom
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.session import provide_session
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup

from ground.util import get_var_raw, get_scoped, deep_get


# ---------------------------------------------------------------------
# internal helpers
# ---------------------------------------------------------------------
_JINJA_PATTERN = re.compile(r"({{.*}}|{%-?.*?-%}|{%.+?%})", re.DOTALL)

def _process_yaml_value(value: Any, name: str) -> Any:
    """
    YAML 값을 Airflow에서 사용할 수 있는 형태로 변환
    - template 키가 있으면: 그 문자열을 그대로 보존 (렌더링은 Airflow에 맡김)
    - NodeDataDto 형태({type,value,isReference} 중 1개 이상 포함)는 '언랩 금지'하고 재귀 처리
    - 딱 {"value": ...} 하나만 있을 때만 언랩 허용
    - 그 외 일반 dict/list/str/primitive는 재귀/그대로
    """
    if value is None:
        return None

    if isinstance(value, dict):
        if "template" in value:
            tmpl = value["template"]
            if not isinstance(tmpl, str):
                raise AirflowException(f"{name}.template must be string, got {type(tmpl).__name__}")
            return tmpl  # 그대로 보존 (렌더는 나중)

        if set(value.keys()) == {"value"}:
            return _process_yaml_value(value["value"], f"{name}.value")

        return {k: _process_yaml_value(v, f"{name}.{k}") for k, v in value.items()}

    if isinstance(value, list):
        return [_process_yaml_value(item, f"{name}[{i}]") for i, item in enumerate(value)]

    if isinstance(value, str):
        # Jinja면 그대로 보존
        if _JINJA_PATTERN.search(value):
            return value
        return value

    # 숫자/불리언 등은 그대로
    return value


def _ensure_mapping(value: Any, name: str) -> Dict[str, Any]:
    """
    kickoff.start_payload, kickoff.start_headers 등은 반드시 dict(매핑)이어야 한다.
    YAML에서 정의된 복잡한 구조를 처리한다.
    """
    if value is None:
        return {}

    # YAML 값 처리
    processed_value = _process_yaml_value(value, name)
    
    if isinstance(processed_value, dict):
        return processed_value

    # 문자열은 허용하지 않음 (Jinja 템플릿도 포함)
    if isinstance(processed_value, str):
        raise AirflowException(
            f"{name} must be a mapping (dict). Got string instead: {processed_value!r}"
        )

    if isinstance(processed_value, (list, tuple)):
        raise AirflowException(
            f"{name} must be a mapping (dict). Got list/tuple instead: {processed_value!r}"
        )

    raise AirflowException(
        f"{name} must be a mapping (dict). Got {type(processed_value).__name__}: {processed_value!r}"
    )


def _get_auth_token(username: str, password: str, auth_conn_id: str = "commerce_os_auth") -> str:
    """
    Commerce OS API 토큰 획득
    """
    from airflow.hooks.base import BaseHook

    # Airflow Connection에서 토큰 URL 가져오기
    conn = BaseHook.get_connection(auth_conn_id)
    token_url = conn.host  # Connection의 Host 필드에 토큰 URL 저장

    data = {
        "grant_type": "password",
        "username": username,
        "password": password,
        "client_id": "commerce-os"
    }

    try:
        import requests
        response = requests.post(token_url, data=data)
        response.raise_for_status()
        token_data = response.json()
        return token_data["access_token"]
    except Exception as e:
        raise AirflowException(f"Failed to get auth token from {token_url}: {e}")


def _make_keys(pipeline: str, **context) -> None:
    """
    한 실행(run) 동안 사용할 키들을 생성해서 XCom에 보관:
      - {pipeline}_idem_key  : Idempotency-Key (kickoff에 사용)
      - {pipeline}_corr_id   : X-Correlation-Id (kickoff/status 공통)
    """
    dag_id = context["dag"].dag_id
    ds_nodash = context["ds_nodash"]
    run_id = context["dag_run"].run_id

    raw = f"{dag_id}:{pipeline}:{ds_nodash}:{run_id}"
    idem_key = hashlib.sha256(raw.encode()).hexdigest()[:32]
    corr_id = f"{run_id}:{pipeline}:{context['ti'].try_number}"

    ti = context["ti"]
    ti.xcom_push(key=f"{pipeline}_idem_key", value=idem_key)
    ti.xcom_push(key=f"{pipeline}_corr_id", value=corr_id)


def _make_failure_callback(
    name: str,
    slack_conn_id: Optional[str],
    error_xcom_key: str,
    payload_backup_key: str,
):
    """
    task 실패 시 Slack으로 요약을 전송하는 on_failure 콜백 팩토리
    """
    def _callback(context):
        # Slack hook 준비
        hook = None
        try:
            # 우선순위: 전달된 인자 → 글로벌 Variable → 스코프 변수 → fallback
            scid = (
                slack_conn_id
                or get_var_raw("task-pipeline_naver_review_slack_webhook_conn_id", None)
                or get_scoped("slack_webhook_conn_id", get_var_raw("var_scope", None), None)
            )
            hook = SlackWebhookHook(slack_webhook_conn_id=scid or "slack_webhook_default")
        except Exception:
            # Slack 미설정이면 조용히 종료
            return

        ti = context["ti"]
        task_id = context["task"].task_id
        exc = context.get("exception")
        err = {}
        job_id = None
        full_payload = None
        raw_exc_text = str(exc) if exc else None

        # AirflowFailException으로 직렬화해 둔 JSON을 복구하거나,
        # 실패 이전에 XCom에 보관한 오류 정보를 읽어온다.
        if exc:
            try:
                parsed = json.loads(str(exc))
                if isinstance(parsed, dict):
                    err = parsed.get("error", {}) or {}
                    job_id = parsed.get("job_id")
                    full_payload = parsed.get("payload")
                raw_exc_text = None
            except (json.JSONDecodeError, TypeError):
                err = ti.xcom_pull(task_ids=task_id, key=error_xcom_key) or {}

        if not err or full_payload is None:
            payload_backup = ti.xcom_pull(task_ids=task_id, key=payload_backup_key) or {}
            if isinstance(payload_backup, dict):
                if not err:
                    err = payload_backup.get("error") or payload_backup
                if full_payload is None:
                    full_payload = payload_backup

        if isinstance(err, str):
            try:
                err = json.loads(err)
            except Exception:
                err = {"raw": err}

        message   = (err.get("message") if isinstance(err, dict) else None) or None
        details   = err.get("details") if isinstance(err, dict) else None
        code      = (err.get("code") if isinstance(err, dict) else None) or None
        retryable = (err.get("retryable") if isinstance(err, dict) else None)
        trace_id  = (err.get("traceId") if isinstance(err, dict) else None)

        lines = [f"*[{name}] {task_id} 실패*"]
        if job_id:
            lines.append(f"jobId: `{job_id}`")
        if code:
            lines.append(f"code: {code}")
        if message:
            lines.append(f"message: {message}")
        if details is not None:
            try:
                details_str = json.dumps(details, ensure_ascii=False, indent=2)
                lines.append(f"details: \n```\n{details_str}\n```")
            except Exception:
                lines.append(f"details: {details}")
        if retryable is not None:
            lines.append(f"retryable: {retryable}")
        if trace_id:
            lines.append(f"traceId: {trace_id}")
        if isinstance(full_payload, dict):
            try:
                payload_str = json.dumps(full_payload, ensure_ascii=False, indent=2)
            except Exception:
                payload_str = str(full_payload)
            if len(payload_str) > 3500:
                payload_str = payload_str[:3500] + "... (truncated)"
            lines.append(f"payload: \n```\n{payload_str}\n```")

        if raw_exc_text:
            lines.append(f"exception: {raw_exc_text}")

        lines.append(f"run_id: {context['dag_run'].run_id}")
        text = "\n".join(lines)

        try:
            if hook:
                hook.send(text=text)
        except Exception:
            context["ti"].log.warning("Slack notification failed")

    return _callback


# ---------------------------------------------------------------------
# public builder
# ---------------------------------------------------------------------
def build_http_async_pipeline(*, dag: DAG, params: Dict[str, Any]) -> TaskGroup:
    """
    HTTP 비동기 패턴(TaskGroup)을 생성한다.
    - DYNAMIC_DAG_SPECS의 params 사용

    params (예)
    ----------
    {
      "name": "naver_review_products_saving",
      "http_conn_id": "http_be_naver_review",

      "kickoff": {
        "start_endpoint": "/v1/jobs",
        "start_method": "POST",
        "start_headers": {"Content-Type":"application/json","Accept":"application/json"},
        "start_payload": {"serviceId":"naver_review_products_saving","payload":{"companyId":4}},
        "start_timeout": 30,
        "job_id_key": "job.id"
      },

      "status": {
        "status_endpoint": "/v1/{job_id}/status",
        "status_method": "GET",
        "status_headers": {"Accept":"application/json"},
        "status_job_id_param": "jobId",
        "completion_key": "job.status",
        "success_statuses": ["DONE","COMPLETED","SUCCEEDED"],
        "fail_statuses": ["FAILED","CANCELLED","ERROR"],
        "poke_interval": 30,
        "timeout": 3600,
        "status_timeout": 30
      },

      "consume": {
        "enabled": true,
        "status_endpoint": "/v1/{job_id}/status",
        "status_method": "GET",
        "status_headers": {"Accept":"application/json"},
        "status_job_id_param": "jobId",
        "completion_key": "job.status",
        "success_statuses": ["DONE","COMPLETED","SUCCEEDED"],
        "fail_statuses": ["FAILED","CANCELLED","ERROR"],
        "poke_interval": 30,
        "timeout": 3600,
        "status_timeout": 30
      },

      "slack_webhook_conn_id": "task-pipeline_naver_review_slack_webhook_conn_id"
    }
    """
    # -------------------------
    # 필수/기본 파라미터
    # -------------------------
    name: str = params.get("name") or "http_async"
    http_conn_id: str = params.get("http_conn_id") or "http_default"

    # kickoff 기본값
    kf = dict(params.get("kickoff") or {})
    start_endpoint: str = kf.get("start_endpoint", "/v1/jobs")
    start_method: str = (kf.get("start_method") or "POST").upper()
    start_headers_raw = _ensure_mapping(kf.get("start_headers"), "kickoff.start_headers")
    start_headers = {k: _process_yaml_value(v, f"kickoff.start_headers.{k}") for k, v in start_headers_raw.items()}
    start_payload: Dict[str, Any] = _process_yaml_value(kf.get("start_payload") or {}, "kickoff.start_payload")
    start_timeout: int = int(kf.get("start_timeout") or 30)
    job_id_key: str = kf.get("job_id_key", "job.id")

    # status 기본값
    st = dict(params.get("status") or {})
    status_endpoint_tpl: str = st.get("status_endpoint", "/v1/{job_id}/status")
    status_method: str = (st.get("status_method") or "GET").upper()
    status_headers_raw = _ensure_mapping(kf.get("start_headers"), "kickoff.status_headers")
    status_headers = {k: _process_yaml_value(v, f"kickoff.status_headers.{k}") for k, v in status_headers_raw.items()}
    job_id_param_key: str = st.get("status_job_id_param", "jobId")
    completion_key: str = st.get("completion_key", "job.status")
    success_statuses = {str(s).upper() for s in (st.get("success_statuses") or ["DONE", "COMPLETED", "SUCCEEDED"])}
    fail_statuses = {str(s).upper() for s in (st.get("fail_statuses") or ["FAILED", "CANCELLED", "ERROR"])}
    poke_interval: int = int(st.get("poke_interval") or 30)
    total_timeout: int = int(st.get("timeout") or 3600)
    single_http_timeout: int = int(st.get("status_timeout") or 30)

    # consume 기본값 (옵션)
    cn = dict(params.get("consume") or {})
    consume_enabled: bool = bool(cn.get("enabled", False))
    consume_status_endpoint_tpl: str = cn.get("status_endpoint", "/v1/{job_id}/status")
    consume_status_method: str = (cn.get("status_method") or "GET").upper()
    consume_status_headers_raw = _ensure_mapping(kf.get("status_headers"), "kickoff.status_headers")
    consume_status_headers = {k: _process_yaml_value(v, f"kickoff.status_headers.{k}") for k, v in consume_status_headers_raw.items()}
    consume_job_id_param_key: str = cn.get("status_job_id_param", "jobId")
    consume_completion_key: str = cn.get("completion_key", "job.status")
    consume_success_statuses = {str(s).upper() for s in (cn.get("success_statuses") or ["DONE", "COMPLETED", "SUCCEEDED"])}
    consume_fail_statuses = {str(s).upper() for s in (cn.get("fail_statuses") or ["FAILED", "CANCELLED", "ERROR"])}
    consume_poke_interval: int = int(cn.get("poke_interval") or poke_interval)
    consume_total_timeout: int = int(cn.get("timeout") or total_timeout)
    consume_single_http_timeout: int = int(cn.get("status_timeout") or single_http_timeout)

    slack_conn_id: Optional[str] = params.get("slack_webhook_conn_id")

    # -------------------------
    # 내부 유틸
    # -------------------------
    def _make_keys(pipeline: str, **context) -> None:
        dag_id = context["dag"].dag_id
        ds_nodash = context["ds_nodash"]
        run_id = context["dag_run"].run_id
        raw = f"{dag_id}:{pipeline}:{ds_nodash}:{run_id}"
        idem_key = hashlib.sha256(raw.encode()).hexdigest()[:32]
        corr_id = f"{run_id}:{pipeline}:{context['ti'].try_number}"
        ti = context["ti"]
        ti.xcom_push(key=f"{pipeline}_idem_key", value=idem_key)
        ti.xcom_push(key=f"{pipeline}_corr_id", value=corr_id)

    def _make_failure_callback(pipeline_name: str, slack_webhook_conn_id: Optional[str], error_xcom_key: str, payload_backup_key: str):
        def _callback(context):
            # Slack hook 준비 (params만 사용)
            hook = None
            if slack_webhook_conn_id:
                try:
                    hook = SlackWebhookHook(slack_webhook_conn_id=slack_webhook_conn_id)
                except Exception:
                    hook = None  # 미설정/오류면 조용히 패스

            ti = context["ti"]
            task_id = context["task"].task_id
            exc = context.get("exception")
            err = {}
            job_id = None
            full_payload = None
            raw_exc_text = str(exc) if exc else None

            if exc:
                try:
                    parsed = json.loads(str(exc))
                    if isinstance(parsed, dict):
                        err = parsed.get("error", {}) or {}
                        job_id = parsed.get("job_id")
                        full_payload = parsed.get("payload")
                    raw_exc_text = None
                except (json.JSONDecodeError, TypeError):
                    err = ti.xcom_pull(task_ids=task_id, key=error_xcom_key) or {}

            if not err or full_payload is None:
                payload_backup = ti.xcom_pull(task_ids=task_id, key=payload_backup_key) or {}
                if isinstance(payload_backup, dict):
                    if not err:
                        err = payload_backup.get("error") or payload_backup
                    if full_payload is None:
                        full_payload = payload_backup

            if isinstance(err, str):
                try:
                    err = json.loads(err)
                except Exception:
                    err = {"raw": err}

            message   = (err.get("message") if isinstance(err, dict) else None) or None
            details   = err.get("details") if isinstance(err, dict) else None
            code      = (err.get("code") if isinstance(err, dict) else None) or None
            retryable = (err.get("retryable") if isinstance(err, dict) else None)
            trace_id  = (err.get("traceId") if isinstance(err, dict) else None)

            lines = [f"*[{pipeline_name}] {task_id} 실패*"]
            if job_id:
                lines.append(f"jobId: `{job_id}`")
            if code:
                lines.append(f"code: {code}")
            if message:
                lines.append(f"message: {message}")
            if details is not None:
                try:
                    details_str = json.dumps(details, ensure_ascii=False, indent=2)
                    lines.append(f"details: \n```\n{details_str}\n```")
                except Exception:
                    lines.append(f"details: {details}")
            if retryable is not None:
                lines.append(f"retryable: {retryable}")
            if trace_id:
                lines.append(f"traceId: {trace_id}")
            if isinstance(full_payload, dict):
                try:
                    payload_str = json.dumps(full_payload, ensure_ascii=False, indent=2)
                except Exception:
                    payload_str = str(full_payload)
                if len(payload_str) > 3500:
                    payload_str = payload_str[:3500] + "... (truncated)"
                lines.append(f"payload: \n```\n{payload_str}\n```")

            if raw_exc_text:
                lines.append(f"exception: {raw_exc_text}")

            lines.append(f"run_id: {context['dag_run'].run_id}")
            text = "\n".join(lines)

            if hook:
                try:
                    hook.send(text=text)
                except Exception:
                    context["ti"].log.warning("Slack notification failed")

        return _callback

    # -------------------------
    # TaskGroup 구성
    # -------------------------
    with TaskGroup(group_id=f"{name}_http_async", dag=dag) as tg:
        start = EmptyOperator(task_id="start")

        # 1) 키 생성
        make_keys = PythonOperator(
            task_id="make_keys",
            python_callable=_make_keys,
            op_kwargs={"pipeline": name},
        )
        IDEM_EXPR = "{{ ti.xcom_pull(task_ids='" + make_keys.task_id + "', key='" + name + "_idem_key') }}"
        CORR_EXPR = "{{ ti.xcom_pull(task_ids='" + make_keys.task_id + "', key='" + name + "_corr_id') }}"

        # 2) 인증 토큰 획득 (Commerce OS API용)
        get_auth_token = PythonOperator(
            task_id="get_auth_token",
            python_callable=_get_auth_token,
            op_kwargs={
                "username": "{{ var.value.commerce_os_username }}",
                "password": "{{ var.value.commerce_os_password }}",
            },
        )

        # 3) kickoff
        kickoff_headers = dict(start_headers or {})
        kickoff_headers.setdefault("Idempotency-Key", IDEM_EXPR)
        kickoff_headers.setdefault("X-Correlation-Id", CORR_EXPR)
        kickoff_headers.setdefault("Content-Type", "application/json")
        kickoff_headers.setdefault("Accept", "application/json")

        error_xcom_key = f"{name}_error"
        error_payload_xcom_key = f"{name}_error_payload"

        def _response_filter(resp):
            try:
                body = resp.json() or {}
            except Exception as e:
                raise AirflowException(f"[{name}][Kickoff] Invalid JSON: {e}")
            job_id = deep_get(body, job_id_key)
            if not job_id:
                raise AirflowException(f"[{name}][Kickoff] Missing job_id at '{job_id_key}'. body={body}")
            return str(job_id)

        kickoff = HttpOperator(
            task_id="kickoff",
            http_conn_id=http_conn_id,
            endpoint=start_endpoint,
            method=start_method,
            headers=kickoff_headers,
            data=json.dumps(start_payload),
            log_response=True,
            response_filter=_response_filter,
            do_xcom_push=True,
            extra_options={"timeout": start_timeout},
            on_failure_callback=_make_failure_callback(name, slack_conn_id, error_xcom_key, error_payload_xcom_key),
        )

        # 3) status polling (HttpSensor)
        if ("{job_id}" in (status_endpoint_tpl or "")) or ("{jobId}" in (status_endpoint_tpl or "")):
            endpoint_templated = status_endpoint_tpl.replace(
                "{job_id}", "{{ ti.xcom_pull(task_ids='" + kickoff.task_id + "') }}"
            ).replace(
                "{jobId}", "{{ ti.xcom_pull(task_ids='" + kickoff.task_id + "') }}"
            )
            request_params = None
        else:
            endpoint_templated = status_endpoint_tpl
            request_params = { (job_id_param_key or "jobId"): "{{ ti.xcom_pull(task_ids='" + kickoff.task_id + "') }}" }

        sensor_headers = dict(status_headers or {})
        sensor_headers.setdefault("X-Correlation-Id", CORR_EXPR)
        sensor_headers.setdefault("Accept", "application/json")

        def _make_response_check(c_key, success_set, fail_set, err_key, err_payload_key):
            def _check(resp, context=None):
                try:
                    payload = resp.json()
                except Exception:
                    return False
                status = str(deep_get(payload, c_key, "")).upper()
                if status in fail_set:
                    # 실패 상세 XCom 저장 + 에러로 변환
                    try:
                        if context and "ti" in context:
                            context["ti"].log.error("[%s] Failure payload: %s", name, payload)
                            context["ti"].xcom_push(key=err_key, value=(payload.get("error") or payload))
                            context["ti"].xcom_push(key=err_payload_key, value=payload)
                    except Exception:
                        pass
                    failure_context = {
                        "job_id": deep_get(payload, "job.id", "N/A"),
                        "error": payload.get("error") or {"message": f"status={status}"},
                        "payload": payload,
                    }
                    try:
                        error_json_str = json.dumps(failure_context)
                    except Exception:
                        error_json_str = f"Could not serialize error payload: {payload}"
                    raise AirflowFailException(error_json_str)
                return status in success_set
            return _check

        _response_check = _make_response_check(
            completion_key, success_statuses, fail_statuses, error_xcom_key, error_payload_xcom_key
        )

        wait = HttpSensor(
            task_id="wait_until_terminal",
            http_conn_id=http_conn_id,
            endpoint=endpoint_templated,
            method=status_method,
            headers=sensor_headers,
            request_params=request_params,
            response_check=_response_check,
            poke_interval=poke_interval,
            timeout=total_timeout,
            mode="reschedule",
            extra_options={"timeout": single_http_timeout},
            on_failure_callback=_make_failure_callback(name, slack_conn_id, error_xcom_key, error_payload_xcom_key),
        )

        # 4) (옵션) consume 단계
        if consume_enabled:
            consume_error_xcom_key = f"{name}_consume_error"
            consume_error_payload_xcom_key = f"{name}_consume_error_payload"

            if ("{job_id}" in (consume_status_endpoint_tpl or "")) or ("{jobId}" in (consume_status_endpoint_tpl or "")):
                consume_endpoint_templated = consume_status_endpoint_tpl.replace(
                    "{job_id}", "{{ ti.xcom_pull(task_ids='" + kickoff.task_id + "') }}"
                ).replace(
                    "{jobId}", "{{ ti.xcom_pull(task_ids='" + kickoff.task_id + "') }}"
                )
                consume_request_params = None
            else:
                consume_endpoint_templated = consume_status_endpoint_tpl
                consume_request_params = { (consume_job_id_param_key or "jobId"): "{{ ti.xcom_pull(task_ids='" + kickoff.task_id + "') }}" }

            def _response_check_consume(resp, context=None):
                try:
                    payload = resp.json()
                except Exception:
                    return False
                status = str(deep_get(payload, consume_completion_key, "")).upper()
                if status in consume_fail_statuses:
                    failure_context = {
                        "job_id": deep_get(payload, "job.id", "N/A"),
                        "error": payload.get("error") or {"message": f"status={status}"},
                        "payload": payload,
                    }
                    try:
                        error_json_str = json.dumps(failure_context)
                    except Exception:
                        error_json_str = f"Could not serialize error payload: {payload}"
                    raise AirflowFailException(error_json_str)
                return status in consume_success_statuses

            # consume 헤더에 Authorization 추가
            consume_headers_with_auth = dict(consume_status_headers or {})
            
            wait_consume = HttpSensor(
                task_id="wait_consume_until_terminal",
                http_conn_id=http_conn_id,
                endpoint=consume_endpoint_templated,
                method=consume_status_method,
                headers=consume_headers_with_auth,
                request_params=consume_request_params,
                response_check=_response_check_consume,
                poke_interval=consume_poke_interval,
                timeout=consume_total_timeout,
                mode="reschedule",
                extra_options={"timeout": consume_single_http_timeout},
                on_failure_callback=_make_failure_callback(name, slack_conn_id, consume_error_xcom_key, consume_error_payload_xcom_key),
            )

        # 5) XCom 정리
        @provide_session
        def _cleanup_keys(
                pipeline: str,
                make_keys_task_id: str,
                kickoff_task_id: str,
                extra_task_ids: list[str] | None = None,
                session=None,
                **context,
        ) -> None:
            """
            - make_keys가 남긴 idempotency/correlation 키
            - kickoff가 남길 수 있는 return_value/xcom_result
            - (옵션) 보조 태스크들이 남긴 XCom도 함께 정리
            """
            dag_id = context["dag"].dag_id
            run_id = getattr(context["dag_run"], "run_id", None)
            execution_date = getattr(context["ti"], "execution_date", None)

            make_keys_to_delete = [f"{pipeline}_idem_key", f"{pipeline}_corr_id"]
            kickoff_keys_to_delete = ["return_value", "xcom_result"]

            # 기본 쿼리 빌더
            def _base_q(task_id: str):
                q = session.query(XCom).filter(
                    XCom.dag_id == dag_id,
                    XCom.task_id == task_id,
                )
                if hasattr(XCom, "run_id") and run_id is not None:
                    q = q.filter(XCom.run_id == run_id)
                else:
                    q = q.filter(XCom.execution_date == execution_date)
                return q

            deleted_make = _base_q(make_keys_task_id).filter(
                XCom.key.in_(make_keys_to_delete)
            ).delete(synchronize_session=False)

            deleted_kick = _base_q(kickoff_task_id).filter(
                XCom.key.in_(kickoff_keys_to_delete)
            ).delete(synchronize_session=False)

            # 보조 태스크 XCom 정리 (있을 때만)
            deleted_extra = 0
            if extra_task_ids:
                for tid in extra_task_ids:
                    deleted_extra += _base_q(tid).delete(synchronize_session=False)

            # 확실히 반영
            session.commit()

            total_deleted = (deleted_make or 0) + (deleted_kick or 0) + (deleted_extra or 0)
            context["ti"].log.info(
                "[%s] Cleanup removed %s XCom rows (make_keys=%s, kickoff=%s, extras=%s)",
                pipeline, total_deleted, make_keys_to_delete, kickoff_keys_to_delete, extra_task_ids
            )

        # ---- 파이프라인 내 배치
        cleanup_keys = PythonOperator(
            task_id="cleanup_keys",
            python_callable=_cleanup_keys,
            op_kwargs={
                "pipeline": name,
                # ★ 반드시 fully-qualified task_id를 넘긴다
                "make_keys_task_id": make_keys.task_id,
                "kickoff_task_id": kickoff.task_id,
                # 보조 태스크(있으면): e.g. modify_payload / prepare_input
                "extra_task_ids": [t.task_id for t in [locals().get("modify_payload"),
                                                       locals().get("prepare_input")] if t],
            },
            trigger_rule=TriggerRule.ALL_DONE,
        )

        end = EmptyOperator(task_id="end")

        # 체인 (보조 태스크가 있는 브랜치/없는 브랜치 모두 end → cleanup_keys 로 마무리)
        if consume_enabled:
            start >> make_keys >> get_auth_token >> kickoff >> wait >> wait_consume >> end >> cleanup_keys
        else:
            start >> make_keys >> get_auth_token >> kickoff >> wait >> end >> cleanup_keys

    return tg


def build_http_async_pipeline_with_input(*, dag: DAG, params: Dict[str, Any], input_data_key: Optional[str] = None) -> TaskGroup:
    """
    이전 stage의 결과를 입력으로 받는 HTTP 비동기 패턴을 생성한다.

    Args:
        dag: DAG 객체
        params: 파이프라인 설정
        input_data_key: 이전 stage에서 가져올 데이터의 XCom 키 (예: "data.output")
    """
    # 입력 데이터가 있는 경우 payload를 동적으로 수정
    if input_data_key:
        # 기존 payload를 가져와서 input_data_key로 데이터 주입
        kf = dict(params.get("kickoff") or {})
        original_payload: Dict[str, Any] = _process_yaml_value(kf.get("start_payload") or {}, "kickoff.start_payload")

        def _modify_payload_with_input(**context):
            # 이전 stage의 결과를 XCom에서 가져오기
            input_data = context['ti'].xcom_pull(task_ids=None, key=input_data_key)

            # payload 복사 및 수정
            modified_payload = original_payload.copy()

            # input[0] 필드에 데이터 주입
            if 'payload' not in modified_payload:
                modified_payload['payload'] = {}
            if 'data' not in modified_payload['payload']:
                modified_payload['payload']['data'] = {}

            modified_payload['payload']['data']['input[0]'] = {
                "type": "Array",
                "isReference": False,
                "value": input_data
            }

            return modified_payload

        # payload 수정을 위한 PythonOperator
        modify_payload = PythonOperator(
            task_id="modify_payload",
            python_callable=_modify_payload_with_input,
            dag=dag
        )

        # kickoff payload를 동적으로 설정하도록 params 수정
        modified_params = params.copy()
        if 'kickoff' not in modified_params:
            modified_params['kickoff'] = {}
        modified_params['kickoff']['start_payload'] = "{{ ti.xcom_pull(task_ids='modify_payload') }}"

        # 수정된 params로 기본 파이프라인 생성
        base_tg = build_http_async_pipeline(dag=dag, params=modified_params)

        # modify_payload를 파이프라인에 연결
        # start >> make_keys >> modify_payload >> kickoff >> wait >> end >> cleanup_keys
        start_task = None
        make_keys_task = None
        kickoff_task = None

        for task in base_tg.children.values():
            if hasattr(task, 'task_id'):
                if task.task_id == 'start':
                    start_task = task
                elif task.task_id == 'make_keys':
                    make_keys_task = task
                elif task.task_id == 'kickoff':
                    kickoff_task = task

        if start_task and make_keys_task and kickoff_task:
            # 기존 연결을 끊고 새로운 연결 생성
            start_task >> make_keys_task >> modify_payload >> kickoff_task

        return base_tg
    else:
        # 입력 데이터가 없으면 기본 파이프라인 생성
        return build_http_async_pipeline(dag=dag, params=params)