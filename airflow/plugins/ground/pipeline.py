# plugins/pipeline.py
from __future__ import annotations

import hashlib
import json
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
from airflow.utils.task_group import TaskGroup

from plugins.ground.util import get_var_raw, get_scoped, deep_get


# ---------------------------------------------------------------------
# internal helpers
# ---------------------------------------------------------------------
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
    start_headers: Dict[str, Any] = dict(kf.get("start_headers") or {"Content-Type": "application/json", "Accept": "application/json"})
    start_payload: Dict[str, Any] = dict(kf.get("start_payload") or {})
    start_timeout: int = int(kf.get("start_timeout") or 30)
    job_id_key: str = kf.get("job_id_key", "job.id")

    # status 기본값
    st = dict(params.get("status") or {})
    status_endpoint_tpl: str = st.get("status_endpoint", "/v1/{job_id}/status")
    status_method: str = (st.get("status_method") or "GET").upper()
    status_headers: Dict[str, Any] = dict(st.get("status_headers") or {"Accept": "application/json"})
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
    consume_status_headers: Dict[str, Any] = dict(cn.get("status_headers") or {"Accept": "application/json"})
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

        # 2) kickoff
        kickoff_headers = dict(start_headers or {})
        kickoff_headers.setdefault("Idempotency-Key", IDEM_EXPR)
        kickoff_headers.setdefault("X-Correlation-Id", CORR_EXPR)
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

            wait_consume = HttpSensor(
                task_id="wait_consume_until_terminal",
                http_conn_id=http_conn_id,
                endpoint=consume_endpoint_templated,
                method=consume_status_method,
                headers=consume_status_headers,
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
        def _cleanup_keys(pipeline: str, make_keys_task_id: str, kickoff_task_id: str, session=None, **context) -> None:
            make_keys_to_delete: List[str] = [f"{pipeline}_idem_key", f"{pipeline}_corr_id"]
            kickoff_keys_to_delete: List[str] = ["return_value", "xcom_result"]
            dag_id = context["dag"].dag_id

            q1 = session.query(XCom).filter(
                XCom.dag_id == dag_id,
                XCom.task_id == make_keys_task_id,
                XCom.key.in_(make_keys_to_delete),
            )
            q2 = session.query(XCom).filter(
                XCom.dag_id == dag_id,
                XCom.task_id == kickoff_task_id,
                XCom.key.in_(kickoff_keys_to_delete),
            )

            if hasattr(XCom, "run_id"):
                q1 = q1.filter(XCom.run_id == context["dag_run"].run_id)
                q2 = q2.filter(XCom.run_id == context["dag_run"].run_id)
            else:
                q1 = q1.filter(XCom.execution_date == context["ti"].execution_date)
                q2 = q2.filter(XCom.execution_date == context["ti"].execution_date)

            deleted_make = q1.delete(synchronize_session=False)
            deleted_kick = q2.delete(synchronize_session=False)
            total_deleted = (deleted_make or 0) + (deleted_kick or 0)
            context["ti"].log.info(
                "[%s] Cleanup removed %s XCom rows (make_keys=%s, kickoff=%s)",
                pipeline,
                total_deleted,
                make_keys_to_delete,
                kickoff_keys_to_delete,
            )

        cleanup_keys = PythonOperator(
            task_id="cleanup_keys",
            python_callable=_cleanup_keys,
            op_kwargs={"pipeline": name, "make_keys_task_id": make_keys.task_id, "kickoff_task_id": "kickoff"},
            trigger_rule="all_done",
        )

        end = EmptyOperator(task_id="end")

        # 체인
        if consume_enabled:
            start >> make_keys >> kickoff >> wait >> wait_consume >> end >> cleanup_keys
        else:
            start >> make_keys >> kickoff >> wait >> end >> cleanup_keys

    return tg