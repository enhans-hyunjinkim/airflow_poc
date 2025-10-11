from __future__ import annotations

from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook

from utils.date_util import now_seoul_str
from utils.string_util import camel_to_snake, pick
from operators.naver_api_operator import NaverApiOperator



# ===============================
# upsert_orders_and_product_orders
# ===============================
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


def upsert_orders_and_product_orders(
        mongo_uri: str,
        db_name: str,
        rows: List[Dict[str, Any]],
        order_coll: str = "woongjin__naver_order",
        product_order_coll: str = "woongjin__naver_product_order",
        batch_size: int = 1000,
) -> Tuple[int, int]:
    client = MongoClient(mongo_uri)
    db = client[db_name]
    col_order = db[order_coll]
    col_porder = db[product_order_coll]

    col_order.create_index("product_order_id", unique=False)
    col_porder.create_index("product_order_id", unique=True)

    now_str = now_seoul_str()

    order_ops: List[UpdateOne] = []
    porder_ops: List[UpdateOne] = []
    for r in rows:
        po_id = r.get("product_order_id")
        if not po_id:
            continue

        order_doc = pick(r, ORDER_KEYS)
        order_doc["collected_at"] = now_str
        order_ops.append(UpdateOne({"product_order_id": po_id}, {"$set": order_doc}, upsert=True))

        p_doc = pick(r, PRODUCT_ORDER_KEYS)
        if not p_doc.get("payment_day"):
            p_date = r.get("payment_date")
            p_doc["payment_day"] = (p_date[:10] if isinstance(p_date, str) and len(p_date) >= 10 else None)
        p_doc["collected_at"] = now_str
        porder_ops.append(UpdateOne({"product_order_id": po_id}, {"$set": p_doc}, upsert=True))

        if len(order_ops) >= batch_size:
            col_order.bulk_write(order_ops, ordered=False)
            order_ops.clear()
        if len(porder_ops) >= batch_size:
            col_porder.bulk_write(porder_ops, ordered=False)
            porder_ops.clear()

    if order_ops:
        col_order.bulk_write(order_ops, ordered=False)
    if porder_ops:
        col_porder.bulk_write(porder_ops, ordered=False)

    return len(rows), len(rows)



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


def upsert_to_mongo(**context):
    ti = context["ti"]
    rows: List[Dict[str, Any]] = ti.xcom_pull(task_ids="add_collected_at") or []
    if not rows:
        print("No rows to upsert.")
        return {"upserted_orders": 0, "upserted_product_orders": 0}

    conn = BaseHook.get_connection("mongo_agent_ground")
    mongo_uri = f"mongodb://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}?authSource=admin"
    db_name = conn.schema

    o_cnt, p_cnt = upsert_orders_and_product_orders(
        mongo_uri=mongo_uri, db_name=db_name, rows=rows
    )
    print(f"Upserted orders={o_cnt}, product_orders={p_cnt}")
    return {"upserted_orders": o_cnt, "upserted_product_orders": p_cnt}


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


def join_and_upsert_stats(**context):
    ti = context["ti"]
    sales_data = ti.xcom_pull(task_ids="process_hourly_sales_stats") or {}
    stats_data = ti.xcom_pull(task_ids="process_hourly_stats") or {}

    if not sales_data or not stats_data:
        print("No data to join")
        return


    if sales_data["aggregate_date"] != stats_data["aggregate_date"]:
        raise RuntimeError("aggregate_date mismatch between sales and stats")

    joined = {**stats_data, **sales_data}

    conn = BaseHook.get_connection("mongo_agent_ground")
    mongo_uri = f"mongodb://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}?authSource=admin"
    db_name = conn.schema
    client = MongoClient(mongo_uri)
    db = client[db_name]
    coll = db["woongjin__purchase_statistics"]
    coll.create_index([("aggregate_date", 1)], unique=True)

    coll.update_one(
        {"aggregate_date": joined["aggregate_date"]},
        {"$set": joined},
        upsert=True,
    )
    print(f"✅ woongjin__purchase_statistics upserted for {joined['aggregate_date']}")
    return joined


def transform_unpayed_rows(**context):
    ti = context["ti"]
    
    # 4일간의 데이터를 각각 수집
    all_raw_data = []
    for day_offset in [4, 3, 2, 1]:
        task_id = f"load_unpayed_orders_day{day_offset}"
        raw_data = ti.xcom_pull(task_ids=task_id) or []
        if raw_data:
            all_raw_data.extend(raw_data)
    
    if not all_raw_data:
        print("No unpayed orders data to transform")
        return []
    
    # 데이터 변환
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
    
    # 추가 변환 로직
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


def upsert_unpayed_to_mongo(**context):
    ti = context["ti"]
    rows: List[Dict[str, Any]] = ti.xcom_pull(task_ids="transform_unpayed_rows") or []
    if not rows:
        print("No unpayed rows to upsert.")
        return {"upserted": 0}

    conn = BaseHook.get_connection("mongo_agent_ground")
    mongo_uri = f"mongodb://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}?authSource=admin"
    db_name = conn.schema

    client = MongoClient(mongo_uri)
    db = client[db_name]
    coll = db["woongjin__unpayed_naver_product_order"]

    coll.create_index(
        [("product_order_id", 1), ("order_date", 1), ("aggregate_date", 1)],
        unique=True,
        name="uq_po_order_aggdate"
    )

    ops = []
    for doc in rows:
        filt = {
            "product_order_id": doc.get("product_order_id"),
            "order_date": doc.get("order_date"),
            "aggregate_date": doc.get("aggregate_date"),
        }
        ops.append(UpdateOne(filt, {"$set": doc}, upsert=True))

    if ops:
        res = coll.bulk_write(ops, ordered=False)
        upserted = (res.upserted_count or 0)
        modified = res.modified_count or 0
        print(f"✅ unpayed upsert: upserted={upserted}, modified={modified}, total_docs={len(rows)}")
        return {"upserted": upserted, "modified": modified, "total_docs": len(rows)}

    return {"upserted": 0, "modified": 0, "total_docs": 0}


# -----------------------
# DAG 정의
# -----------------------
default_args = {"owner": "data-connector", "depends_on_past": False}

with DAG(
        dag_id="woongjin_naver_promotion_dag",
        default_args=default_args,
        description="네이버 스토어 주문 수집 → 변환 → MongoDB 적재",
        schedule="@daily",
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

    upsert = PythonOperator(
        task_id="upsert_to_mongo",
        python_callable=upsert_to_mongo,
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

    join_and_upsert = PythonOperator(
        task_id="join_and_upsert_stats",
        python_callable=join_and_upsert_stats,
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

    upsert_unpayed = PythonOperator(
        task_id="upsert_unpayed_to_mongo",
        python_callable=upsert_unpayed_to_mongo,
    )

    end = EmptyOperator(task_id="end")


    # 주문 데이터 처리 파이프라인
    start >> fetch_orders >> transform_orders >> add_collected_at >> upsert
    
    # 통계 데이터 처리 파이프라인
    upsert >> [collect_sales, collect_stats]
    collect_sales >> process_sales
    collect_stats >> process_stats
    [process_sales, process_stats] >> join_and_upsert
    
    # 미결제 주문 처리 파이프라인 (4일간의 데이터를 병렬로 수집)
    join_and_upsert >> load_unpayed_orders_tasks >> transform_unpayed >> upsert_unpayed >> end