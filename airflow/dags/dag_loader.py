# dags/dag_loader.py
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pendulum
import yaml
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# 공용 유틸 (Variable/JSON 헬퍼)
from ground.util import get_var_raw

# type별 빌더 레지스트리 (예: "http_async" -> build_http_async_pipeline)
from ground.registry import get_builder


# -----------------------------
# 스펙 로딩 (YAML 파일 우선, JSON Variable fallback)
# -----------------------------
def _load_specs_from_yaml() -> List[Dict[str, Any]]:
    """YAML 파일에서 DAG 설정을 로드"""
    yaml_paths = [
        # dags 폴더의 YAML 파일 (우선)
        os.path.join(os.path.dirname(__file__), "DYNAMIC_DAG_SPECS.yaml"),
        # 프로젝트 루트의 YAML 파일 (fallback)
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "DYNAMIC_DAG_SPECS.yaml"),
    ]
    
    for yaml_path in yaml_paths:
        if os.path.exists(yaml_path):
            try:
                with open(yaml_path, 'r', encoding='utf-8') as f:
                    data = yaml.safe_load(f)
                    if isinstance(data, list):
                        return data
                    elif isinstance(data, dict):
                        return [data]  # 단일 DAG 설정인 경우 리스트로 변환
                    else:
                        return []
            except Exception as e:
                print(f"YAML 파일 로딩 실패 ({yaml_path}): {e}")
                continue
    
    return []


def _load_specs_from_variables() -> List[Dict[str, Any]]:
    """기존 JSON Variable에서 DAG 설정을 로드 (fallback)"""
    raw = get_var_raw("DYNAMIC_DAG_SPECS", "[]")
    try:
        data = json.loads(raw or "[]")
        if not isinstance(data, list):
            return []
        return data
    except Exception:
        return []


# -----------------------------
# 기본 args 가공
# -----------------------------
def _build_default_args(spec_args: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    tz = pendulum.timezone("Asia/Seoul")

    # 전역 기본값
    base = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(
            minutes=int(get_var_raw("dag_retry_delay_minutes", "5") or 5)
        ),
    }

    spec_args = spec_args or {}

    # start_date 처리 (YYYY-MM-DD 형태 권장)
    sd = spec_args.get("start_date")
    if isinstance(sd, str):
        try:
            y, m, d = [int(x) for x in sd.split("-")]
            base["start_date"] = datetime(y, m, d, tzinfo=tz)
        except Exception:
            # 파싱 실패 시 오늘 날짜로 fallback
            base["start_date"] = datetime(2025, 1, 1, tzinfo=tz)
    elif isinstance(sd, (datetime,)):
        base["start_date"] = sd
    else:
        # Variable 형식에 없으면 기본값
        base["start_date"] = datetime(2025, 1, 1, tzinfo=tz)

    # retries / retry_delay 오버라이드(선택)
    if "retries" in spec_args:
        base["retries"] = int(spec_args["retries"])
    if "retry_delay_minutes" in spec_args:
        base["retry_delay"] = timedelta(minutes=int(spec_args["retry_delay_minutes"]))

    if "owner" in spec_args:
        base["owner"] = spec_args["owner"]

    if "depends_on_past" in spec_args:
        base["depends_on_past"] = bool(spec_args["depends_on_past"])

    return base


# -----------------------------
# DAG 빌드
# -----------------------------
def _build_dag_from_spec(spec: Dict[str, Any]) -> Optional[DAG]:
    """
    하나의 스펙(JSON dict)으로부터 DAG 객체를 생성.
    """
    dag_id = spec.get("dag_id")
    if not dag_id:
        return None

    schedule = spec.get("schedule") or None
    tags = spec.get("tags") or []
    default_args = _build_default_args(spec.get("default_args"))

    dag = DAG(
        dag_id=dag_id,
        description=spec.get("description") or dag_id,
        default_args=default_args,
        schedule=schedule,
        catchup=bool(spec.get("catchup", False)),
        max_active_runs=int(spec.get("max_active_runs", 1)),
        dagrun_timeout=timedelta(hours=int(spec.get("dagrun_timeout_hours", 1))),
        tags=tags,
    )

    with dag:
        # 공통 시작/끝
        start_all = EmptyOperator(task_id="start_all", dag=dag)
        end_all = EmptyOperator(task_id="end_all", dag=dag)

        # stage들 생성
        prev = start_all

        for stage in (spec.get("stages") or []):
            stage_type = stage.get("type")  # 예: "http_async", "http_async_with_input" 등
            stage_id = stage.get("id") or stage_type or "stage"
            params = stage.get("params") or {}
            input_data_key = stage.get("input_data_key")  # 이전 stage의 결과를 가져올 XCom 키

            builder = get_builder(stage_type)
            if not builder:
                # 알 수 없는 type이면 스킵
                continue

            # input_data_key가 있는 경우 특별한 빌더 사용
            if input_data_key and stage_type == "http_async":
                # http_async_with_input 빌더 사용
                input_builder = get_builder("http_async_with_input")
                if input_builder:
                    built = input_builder(dag=dag, params=params, input_data_key=input_data_key)
                else:
                    built = builder(dag=dag, params=params)
            else:
                built = builder(dag=dag, params=params)

            # TaskGroup 또는 BaseOperator 모두 >> 연산자로 연결 가능
            prev >> built
            prev = built

        prev >> end_all
    return dag


# -----------------------------
# DAG 등록 (Airflow는 이 파일 import 시점에 globals() 내 DAG들을 스캔)
# -----------------------------
# YAML 파일을 우선 사용, 없으면 JSON Variable 사용
_specs = _load_specs_from_yaml() or _load_specs_from_variables()

for _spec in _specs:
    _dag = _build_dag_from_spec(_spec)
    if _dag:
        globals()[_dag.dag_id] = _dag