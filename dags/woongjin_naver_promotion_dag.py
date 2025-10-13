from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from utils.date_util import now_seoul_str
from utils.string_util import camel_to_snake, pick
from operators.naver_api_operator import NaverApiOperator
from operators.mongo_operator import MongoUpsertOperator



ORDER_KEYS = [
    "product_order_id",
    "charge_amount_payment_amount",
    "checkout_accumulation_payment_amount",
    "general_payment_amount",
    "naver_mileage_payment_amount",
    "order_date",
    "order_discount_amount",
    "order_id",
    "orderer_id",
    "orderer_name",
    "orderer_tel",
    "payment_date",
    "payment_due_date",
    "payment_means",
    "is_delivery_memo_particular_input",
    "pay_location_type",
    "orderer_no",
    "pay_later_payment_amount",
]

PRODUCT_ORDER_KEYS = [
    "product_order_id",
    "payment_date",
    "payment_day",
    "claim_status",
    "claim_type",
    "decision_date",
    "delivery_discount_amount",
    "delivery_fee_amount",
    "product_id",
    "product_name",
    "product_order_status",
    "quantity",
    "total_payment_amount",
    "expected_settlement_amount",
    "logistics_company_id",
    "logistics_center_id",
]


def transform_orders_data(**context):
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids="fetch_orders") or []

    if not raw_data:
        print("No data to transform")
        return []

    if isinstance(raw_data, list):
        rows = raw_data
    else:
        contents = raw_data.get("data", {}).get("contents", [])
        rows = contents

    transformed_rows = []
    for item in rows:
        row: Dict[str, Any] = {}
        row["product_order_id"] = item.get("productOrderId")
        order = item.get("content", {}).get("order", {})
        for k, v in order.items():
            snake = camel_to_snake(k)
            row[snake] = v
            if k == "paymentDate" and isinstance(v, str) and len(v) >= 10:
                row["payment_day"] = v[:10]
        product_order = item.get("content", {}).get("productOrder", {})
        for k, v in product_order.items():
            row[camel_to_snake(k)] = v
        transformed_rows.append(row)

    print(f"Transformed {len(transformed_rows)} orders")
    return transformed_rows


def add_collected_at(**context):
    ti = context["ti"]
    rows: List[Dict[str, Any]] = ti.xcom_pull(task_ids="transform_orders_data") or []
    collected_at = now_seoul_str()

    for r in rows:
        r["collected_at"] = collected_at

    return rows


def prepare_order_data(**context):
    ti = context["ti"]
    rows: List[Dict[str, Any]] = ti.xcom_pull(task_ids="add_collected_at") or []

    if not rows:
        return []

    order_data = []
    for r in rows:
        po_id = r.get("product_order_id")
        if not po_id:
            continue

        order_doc = pick(r, ORDER_KEYS)
        order_doc["collected_at"] = now_seoul_str()
        order_data.append(order_doc)

    return order_data


def prepare_product_order_data(**context):
    ti = context["ti"]
    rows: List[Dict[str, Any]] = ti.xcom_pull(task_ids="add_collected_at") or []

    if not rows:
        return []

    product_order_data = []
    for r in rows:
        po_id = r.get("product_order_id")
        if not po_id:
            continue

        p_doc = pick(r, PRODUCT_ORDER_KEYS)
        if not p_doc.get("payment_day"):
            p_date = r.get("payment_date")
            p_doc["payment_day"] = (p_date[:10] if isinstance(p_date, str) and len(p_date) >= 10 else None)
        p_doc["collected_at"] = now_seoul_str()
        product_order_data.append(p_doc)

    return product_order_data


def process_hourly_sales_stats(**context):
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids="collect_hourly_sales_stats") or {}

    if not raw_data:
        print("No sales stats data to process")
        return {}

    rows = raw_data.get("rows") or []
    total_purchases = sum(float(r.get("numPurchases") or 0) for r in rows)

    data_interval_start = context.get('data_interval_start', datetime.now())
    if isinstance(data_interval_start, str):
        data_interval_start = datetime.fromisoformat(data_interval_start.replace('Z', '+00:00'))

    exec_dt = data_interval_start - timedelta(days=1)
    date_str = exec_dt.strftime("%Y-%m-%d")

    result = {"aggregate_date": date_str, "total_purchases": total_purchases}
    return result


def process_hourly_stats(**context):
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids="collect_hourly_stats") or {}

    if not raw_data:
        print("No stats data to process")
        return {}


    rows = raw_data.get("rows") or []
    num_interactions = sum(float(r.get("numInteractions") or 0) for r in rows)
    num_purchases = sum(float(r.get("numPurchases") or 0) for r in rows)
    pay_amount = sum(float(r.get("payAmount") or 0) for r in rows)


    data_interval_start = context.get('data_interval_start', datetime.now())
    if isinstance(data_interval_start, str):
        data_interval_start = datetime.fromisoformat(data_interval_start.replace('Z', '+00:00'))

    exec_dt = data_interval_start - timedelta(days=1)
    date_str = exec_dt.strftime("%Y-%m-%d")

    result = {
        "aggregate_date": date_str,
        "collected_at": now_seoul_str(),
        "num_interactions": num_interactions,
        "num_purchases": num_purchases,
        "pay_amount": pay_amount,
    }
    return result


def prepare_stats_data(**context):
    ti = context["ti"]
    sales_data = ti.xcom_pull(task_ids="process_hourly_sales_stats") or {}
    stats_data = ti.xcom_pull(task_ids="process_hourly_stats") or {}

    if not sales_data or not stats_data:
        print("No data to join")
        return {}

    if sales_data["aggregate_date"] != stats_data["aggregate_date"]:
        raise RuntimeError("aggregate_date mismatch between sales and stats")

    joined = {**stats_data, **sales_data}
    return joined


def transform_unpayed_rows(**context):
    ti = context["ti"]

    all_raw_data = []
    for day_offset in [4, 3, 2, 1]:
        task_id = f"load_unpayed_orders_day{day_offset}"
        raw_data = ti.xcom_pull(task_ids=task_id) or []
        if raw_data:
            all_raw_data.extend(raw_data)

    if not all_raw_data:
        print("No unpayed orders data to transform")
        return []


    transformed_rows = []
    for item in all_raw_data:
        row: Dict[str, Any] = {}
        row["product_order_id"] = item.get("productOrderId")
        order = item.get("content", {}).get("order", {})
        for k, v in order.items():
            snake = camel_to_snake(k)
            row[snake] = v
        product_order = item.get("content", {}).get("productOrder", {})
        for k, v in product_order.items():
            row[camel_to_snake(k)] = v
        transformed_rows.append(row)


    out: List[Dict[str, Any]] = []
    collected_at = now_seoul_str()

    for r in transformed_rows:
        product_order_id = r.get("product_order_id")
        product_id = r.get("product_id")
        order_date = r.get("order_date")
        payment_due_date = r.get("payment_due_date")

        if isinstance(order_date, str) and len(order_date) >= 10:
            aggregate_date = order_date[:10]
        elif isinstance(payment_due_date, str) and len(payment_due_date) >= 10:
            aggregate_date = payment_due_date[:10]
        else:
            aggregate_date = None

        out.append({
            "product_order_id": product_order_id,
            "product_id": product_id,
            "order_date": order_date,
            "payment_due_date": payment_due_date,
            "aggregate_date": aggregate_date,
            "collected_at": collected_at,
        })

    print(f"Transformed {len(out)} unpayed orders from 4 days")
    return out



# -----------------------
# DAG 정의
# -----------------------
default_args = {"owner": "data-connector", "depends_on_past": False}

with DAG(
        dag_id="woongjin_naver_promotion_dag",
        default_args=default_args,
        description="네이버 스토어 주문 수집 → 변환 → MongoDB 적재",
        schedule="15 20 * * *",
        start_date=datetime(2025, 9, 30),
        catchup=False,
        tags=["woongjin", "naver-promotion"],
) as dag:

    start = EmptyOperator(task_id="start")

    fetch_orders = NaverApiOperator(
        task_id="fetch_orders",
        endpoint="/v1/pay-order/seller/product-orders",
        method="GET",
        parameters={
            "from": "{{ (data_interval_start - macros.timedelta(days=1)).strftime('%Y-%m-%dT00:00:00.000+09:00') }}",
            "to": "{{ (data_interval_start - macros.timedelta(days=1)).strftime('%Y-%m-%dT23:59:59.999+09:00') }}",
            "rangeType": "PAYED_DATETIME"
        },
        fetch_all=True,
        pagination_config={
            "page_param": "page",
            "page_size_param": "pageSize",
            "page_size": 100,
            "start_page": 1,
            "has_next_key": "data.pagination.hasNext",
            "contents_key": "data.contents"
        },
        conn_id="woongjin_naver_api",
        log_response=True
    )

    transform_orders = PythonOperator(
        task_id="transform_orders_data",
        python_callable=transform_orders_data,
    )

    add_collected_at = PythonOperator(
        task_id="add_collected_at",
        python_callable=add_collected_at,
    )

    prepare_orders = PythonOperator(
        task_id="prepare_orders",
        python_callable=prepare_order_data,
    )

    prepare_product_orders = PythonOperator(
        task_id="prepare_product_orders",
        python_callable=prepare_product_order_data,
    )

    upsert_orders = MongoUpsertOperator(
        task_id="upsert_orders",
        conn_id="mongo_agent_ground",
        collection="woongjin__naver_order",
        documents="{{ ti.xcom_pull(task_ids='prepare_orders') }}",
        filter_fields=["product_order_id", "payment_date"],
        many=True
    )

    upsert_product_orders = MongoUpsertOperator(
        task_id="upsert_product_orders",
        conn_id="mongo_agent_ground",
        collection="woongjin__naver_product_order",
        documents="{{ ti.xcom_pull(task_ids='prepare_product_orders') }}",
        filter_fields=["product_order_id", "payment_day"],
        many=True
    )

    collect_sales = NaverApiOperator(
        task_id="collect_hourly_sales_stats",
        endpoint="/v1/bizdata-stats/channels/{{ var.value.woongjin_naver_stat_channel_no }}/sales/hourly/detail",
        method="GET",
        parameters={
            "startDate": "{{ (data_interval_start - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            "endDate": "{{ (data_interval_start - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
        },
        fetch_all=False,
        conn_id="woongjin_naver_stat_api",
        log_response=True
    )

    collect_stats = NaverApiOperator(
        task_id="collect_hourly_stats",
        endpoint="/v1/bizdata-stats/channels/{{ var.value.woongjin_naver_stat_channel_no }}/marketing/hourly/simple",
        method="GET",
        parameters={
            "startDate": "{{ (data_interval_start - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            "endDate": "{{ (data_interval_start - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
        },
        fetch_all=False,
        conn_id="woongjin_naver_stat_api",
        log_response=True
    )

    process_sales = PythonOperator(
        task_id="process_hourly_sales_stats",
        python_callable=process_hourly_sales_stats,
    )

    process_stats = PythonOperator(
        task_id="process_hourly_stats",
        python_callable=process_hourly_stats,
    )

    prepare_stats = PythonOperator(
        task_id="prepare_stats",
        python_callable=prepare_stats_data,
    )


    upsert_stats = MongoUpsertOperator(
        task_id="upsert_stats",
        conn_id="mongo_agent_ground",
        collection="woongjin__purchase_statistics",
        documents="{{ ti.xcom_pull(task_ids='prepare_stats') }}",
        filter_fields=["aggregate_date"],
        many=False
    )


    load_unpayed_orders_tasks = []
    for day_offset in [4, 3, 2, 1]:
        task = NaverApiOperator(
            task_id=f"load_unpayed_orders_day{day_offset}",
            endpoint="/v1/pay-order/seller/product-orders",
            method="GET",
            parameters={
                "from": f"{{{{ (data_interval_start - macros.timedelta(days={day_offset})).strftime('%Y-%m-%dT00:00:00.000+09:00') }}}}",
                "to": f"{{{{ (data_interval_start - macros.timedelta(days={day_offset})).strftime('%Y-%m-%dT23:59:59.999+09:00') }}}}",
                "rangeType": "ORDERED_DATETIME",
                "productOrderStatuses": "PAYMENT_WAITING"
            },
            fetch_all=True,
            pagination_config={
                "page_param": "page",
                "page_size_param": "pageSize",
                "page_size": 100,
                "start_page": 1,
                "has_next_key": "data.pagination.hasNext",
                "contents_key": "data.contents"
            },
            conn_id="woongjin_naver_api",
            log_response=True
        )
        load_unpayed_orders_tasks.append(task)

    transform_unpayed = PythonOperator(
        task_id="transform_unpayed_rows",
        python_callable=transform_unpayed_rows,
    )

    upsert_unpayed = MongoUpsertOperator(
        task_id="upsert_unpayed",
        conn_id="mongo_agent_ground",
        collection="woongjin__unpayed_naver_product_order",
        documents="{{ ti.xcom_pull(task_ids='transform_unpayed_rows') }}",
        filter_fields=["product_order_id", "order_date", "aggregate_date"],
        many=True
    )

    end = EmptyOperator(task_id="end")


    # 주문 데이터 처리 파이프라인
    start >> fetch_orders >> transform_orders >> add_collected_at >> [prepare_orders, prepare_product_orders]
    prepare_orders >> upsert_orders
    prepare_product_orders >> upsert_product_orders

    # 통계 데이터 처리 파이프라인
    upsert_orders >> [collect_sales, collect_stats]
    upsert_product_orders >> [collect_sales, collect_stats]
    collect_sales >> process_sales
    collect_stats >> process_stats
    process_sales >> prepare_stats
    process_stats >> prepare_stats
    prepare_stats >> upsert_stats

    # 미결제 주문 처리 파이프라인 (4일간의 데이터를 병렬로 수집)
    upsert_stats >> load_unpayed_orders_tasks >> transform_unpayed >> upsert_unpayed >> end