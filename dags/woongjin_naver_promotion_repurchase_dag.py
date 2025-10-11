from __future__ import annotations

from datetime import datetime, timedelta, date
from typing import Any, Dict, List

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from utils.date_util import now_seoul_str
from utils.string_util import camel_to_snake
from operators.naver_api_operator import NaverApiOperator
from operators.mongo_operator import MongoUpsertOperator


def transform_repurchase(**context):
    ti = context["ti"]
    src: List[Dict[str, Any]] = ti.xcom_pull(task_ids="fetch_repurchase") or []
    out: List[Dict[str, Any]] = []
    collected_at = now_seoul_str()

    for node in src:
        snake = {}
        for k, v in (node or {}).items():
            snake[camel_to_snake(k)] = v


        agg = node.get("aggregateDate") or node.get("aggregate_date")
        if not agg:
            continue

        try:
            start_d = date.fromisoformat(str(agg)[:10])
        except Exception:
            continue

        for i in range(7):
            d = start_d + timedelta(days=i)
            new_doc = dict(snake)
            new_doc["aggregate_date"] = d.isoformat()
            new_doc["collected_at"] = collected_at
            out.append(new_doc)

    return out


# -----------------------
# DAG 정의
# -----------------------
default_args = {"owner": "data-connector", "depends_on_past": False}

with DAG(
        dag_id="woongjin_naver_promotion_repurchase_dag",
        default_args=default_args,
        description="재구매 통계 수집 (Operator 사용) → 변환(7일 확장) → MongoDB upsert",
        schedule="@weekly",
        start_date=datetime(2025, 9, 30),
        catchup=False,
        tags=["woongjin", "naver-repurchase"],
) as dag:

    start = EmptyOperator(task_id="start")


    fetch_repurchase = NaverApiOperator(
        task_id="fetch_repurchase",
        endpoint="/v1/customer-data/repurchase/account/statistics",
        method="GET",
        parameters={
            "startDate": "{{ (data_interval_start - macros.timedelta(days=6)).strftime('%Y-%m-%d') }}",
            "endDate": "{{ data_interval_start.strftime('%Y-%m-%d') }}"
        },
        fetch_all=False,
        conn_id="woongjin_naver_stat_api",
        log_response=True
    )

    # 데이터 변환
    transform = PythonOperator(
        task_id="transform_repurchase",
        python_callable=transform_repurchase,
    )

    # MongoUpsertOperator로 MongoDB 저장 교체
    upsert_repurchase = MongoUpsertOperator(
        task_id="upsert_repurchase",
        conn_id="mongo_agent_ground",
        collection="woongjin__repurchase_statistics",
        documents="{{ ti.xcom_pull(task_ids='transform_repurchase') }}",
        filter_fields=["aggregate_date"],
        many=True
    )

    end = EmptyOperator(task_id="end")

    start >> fetch_repurchase >> transform >> upsert_repurchase >> end
