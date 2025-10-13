from __future__ import annotations

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime, timedelta

from utils.date_util import parse_datetime_flexible, calc_period, to_instant_range
from operators.mongo_operator import MongoFindOperator
from operators.http_operator import HttpPostOperator

# ===== 설정 =====
MONGO_CONN_ID = "mongo_agent_ground"
AI_API_CONN_ID = "ai_be_api"


# ===== 데이터 처리 함수들 =====
def prepare_review_data(**context):
    ti = context["ti"]
    reviews = ti.xcom_pull(task_ids="load_reviews") or []

    ds = context["ds"]
    d = datetime.strptime(ds, "%Y-%m-%d").date()
    start_day = (d - timedelta(days=6)).strftime("%Y-%m-%d")
    end_day = d.strftime("%Y-%m-%d")

    processed_reviews = []
    for doc in reviews:
        processed_reviews.append({
            "review_id": str(doc.get("review_id", "")),
            "product_id": str(doc.get("product_id", "")),
            "created_at": doc.get("created_at") or doc.get("createdAt"),
            "created_day": str(doc.get("created_day", "")),
            "category_id": doc.get("category_id"),
        })

    ti.xcom_push(key="reviews", value=processed_reviews)
    ti.xcom_push(key="start_date", value=start_day)
    ti.xcom_push(key="report_type", value="WEEKLY")

    return {"count": len(processed_reviews), "start_day": start_day, "end_day": end_day, "report_type": "WEEKLY"}

def prepare_analysis_payloads(**context):
    """분석용 페이로드 준비"""
    ti = context["ti"]
    reviews = ti.xcom_pull(key="reviews", task_ids="prepare_review_data") or []

    if not reviews:
        print("No reviews found for analysis")
        return {"enqueued": 0, "payloads": []}

    payloads = [{"review_id": r["review_id"]} for r in reviews if r.get("review_id")]

    if not payloads:
        print("No valid review_ids found for analysis")
        return {"enqueued": 0, "payloads": []}

    print(f"Prepared {len(payloads)} analysis payloads")
    return {"enqueued": len(payloads), "payloads": payloads}

def prepare_product_report_payloads(**context):
    """상품 리포트용 페이로드 준비"""
    ti = context["ti"]
    reviews = ti.xcom_pull(key="reviews", task_ids="prepare_review_data") or []
    report_type = ti.xcom_pull(key="report_type", task_ids="prepare_review_data") or "WEEKLY"
    start_date_input = ti.xcom_pull(key="start_date", task_ids="prepare_review_data")

    start_d, end_d = calc_period(report_type, start_date_input)
    start_inst, end_inst = to_instant_range(start_d, end_d)
    start_date = start_d.strftime("%Y-%m-%d")
    end_date = end_d.strftime("%Y-%m-%d")

    filtered = []
    for r in reviews:
        dt = parse_datetime_flexible(r.get("created_at"))
        if dt and (start_inst <= dt < end_inst):
            filtered.append(r)

    product_ids = sorted({r.get("product_id") for r in filtered if r.get("product_id")})
    payloads = [
        {
            "product_id": pid,
            "report_type": report_type,
            "start_date": start_date,
            "end_date": end_date,
            "client": "WOONGJIN",
            "is_brand": False,
        }
        for pid in product_ids
    ]

    return {"enqueued": len(payloads), "payloads": payloads}

def prepare_brand_report_payloads(**context):
    """브랜드 리포트용 페이로드 준비"""
    ti = context["ti"]
    reviews = ti.xcom_pull(key="reviews", task_ids="prepare_review_data") or []
    report_type = ti.xcom_pull(key="report_type", task_ids="prepare_review_data") or "WEEKLY"
    start_date_input = ti.xcom_pull(key="start_date", task_ids="prepare_review_data")

    start_d, end_d = calc_period(report_type, start_date_input)
    start_inst, end_inst = to_instant_range(start_d, end_d)
    start_date = start_d.strftime("%Y-%m-%d")
    end_date = end_d.strftime("%Y-%m-%d")

    # MongoDB에서 브랜드 ID 조회
    from airflow.providers.mongo.hooks.mongo import MongoHook
    mongo = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    cat_coll = mongo.get_collection("woongjin__product_category")

    valid_brand_ids = []
    for doc in cat_coll.find({"parent_category_id": {"$nin": [None, ""]}}, {"_id": 1}):
        _id = doc.get("_id")
        if _id is None:
            continue
        try:
            from bson import ObjectId
            if isinstance(_id, ObjectId):
                valid_brand_ids.append(_id.binary.hex())
            else:
                valid_brand_ids.append(str(_id))
        except Exception:
            valid_brand_ids.append(str(_id))

    valid_brand_ids = list({x.strip() for x in valid_brand_ids if x and x.strip()})

    filtered = []
    for r in reviews:
        dt = parse_datetime_flexible(r.get("created_at"))
        if not (dt and (start_inst <= dt < end_inst)):
            continue
        cid = r.get("category_id")

        cid_str = None
        if cid is None:
            continue
        try:
            from bson import ObjectId
            if isinstance(cid, ObjectId):
                cid_str = str(cid)
            else:
                cid_str = str(cid)
        except Exception:
            cid_str = str(cid)
        if cid_str in valid_brand_ids:
            filtered.append(cid_str)

    brand_ids = sorted(set(filtered))
    payloads = [
        {
            "product_id": bid,
            "report_type": report_type,
            "start_date": start_date,
            "end_date": end_date,
            "client": "WOONGJIN",
            "is_brand": True,
        }
        for bid in brand_ids
    ]

    if not payloads:
        print("No brand_ids to report")
        return {"enqueued": 0, "payloads": []}

    return {"enqueued": len(payloads), "payloads": payloads}

def prepare_period_report_payloads(**context):
    """기간 리포트용 페이로드 준비"""
    ti = context["ti"]
    report_type = ti.xcom_pull(key="report_type", task_ids="prepare_review_data") or "WEEKLY"
    start_date_input = ti.xcom_pull(key="start_date", task_ids="prepare_review_data")

    start_d, end_d = calc_period(report_type, start_date_input)
    start_date = start_d.strftime("%Y-%m-%d")
    end_date = end_d.strftime("%Y-%m-%d")

    payloads = [
        {
            "report_type": report_type,
            "start_date": start_date,
            "end_date": end_date,
            "client": "WOONGJIN",
            "is_brand": False,
        }
    ]

    return {"enqueued": 1, "payloads": payloads}

# ===== DAG =====
default_args = {"owner": "data-connector", "depends_on_past": False}

with DAG(
        dag_id="woongjin_naver_review_weekly_trigger_dag",
        default_args=default_args,
        description="Load last week's reviews → trigger weekly analysis/report generation",
        schedule="0 18 * * 0",
        start_date=datetime(2025, 10, 1),
        catchup=False,
        tags=["woongjin", "naver-review", "report", "weekly"],
        render_template_as_native_obj=True,
) as dag:

    start = EmptyOperator(task_id="start")

    # MongoDB에서 리뷰 데이터 조회 (전주 데이터)
    load_reviews = MongoFindOperator(
        task_id="load_reviews",
        conn_id=MONGO_CONN_ID,
        collection="woongjin__product_review_analysis",
        query={"created_day": {"$gte": "{{ macros.ds_add(ds, -7) }}", "$lt": "{{ ds }}"}},
        projection={
            "review_id": 1,
            "product_id": 1,
            "created_at": 1,
            "created_day": 1,
            "category_id": 1
        }
    )

    # 리뷰 데이터 처리
    prepare_data = PythonOperator(
        task_id="prepare_review_data",
        python_callable=prepare_review_data
    )

    # 분석용 페이로드 준비
    prepare_analysis = PythonOperator(
        task_id="prepare_analysis_payloads",
        python_callable=prepare_analysis_payloads
    )

    # 상품 리포트용 페이로드 준비
    prepare_product_report = PythonOperator(
        task_id="prepare_product_report_payloads",
        python_callable=prepare_product_report_payloads
    )

    # 브랜드 리포트용 페이로드 준비
    prepare_brand_report = PythonOperator(
        task_id="prepare_brand_report_payloads",
        python_callable=prepare_brand_report_payloads
    )

    # 기간 리포트용 페이로드 준비
    prepare_period_report = PythonOperator(
        task_id="prepare_period_report_payloads",
        python_callable=prepare_period_report_payloads
    )

    # AI 분석 트리거
    trigger_analysis = HttpPostOperator(
        task_id="trigger_analysis",
        http_conn_id=AI_API_CONN_ID,
        endpoint="llm-tasks/enqueue",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        data={
            "source": "cos-woongjin",
            "request_type": "WOONGJIN_REVIEW_POINTS_EXTRACTION",
            "payloads": "{{ ti.xcom_pull(task_ids='prepare_analysis_payloads').get('payloads', []) }}"
        }
    )

    # 상품 리포트 트리거
    trigger_product_report = HttpPostOperator(
        task_id="trigger_product_report",
        http_conn_id=AI_API_CONN_ID,
        endpoint="llm-tasks/enqueue",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        data={
            "source": "cos-woongjin",
            "request_type": "REVIEW_REPORT_GENERATION",
            "payloads": "{{ ti.xcom_pull(task_ids='prepare_product_report_payloads').get('payloads', []) }}"
        }
    )

    # 브랜드 리포트 트리거
    trigger_brand_report = HttpPostOperator(
        task_id="trigger_brand_report",
        http_conn_id=AI_API_CONN_ID,
        endpoint="llm-tasks/enqueue",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        data={
            "source": "cos-woongjin",
            "request_type": "REVIEW_REPORT_GENERATION",
            "payloads": "{{ ti.xcom_pull(task_ids='prepare_brand_report_payloads').get('payloads', []) }}"
        }
    )

    # 기간 리포트 트리거
    trigger_period_report = HttpPostOperator(
        task_id="trigger_period_report",
        http_conn_id=AI_API_CONN_ID,
        endpoint="llm-tasks/enqueue",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        data={
            "source": "cos-woongjin",
            "request_type": "WOONGJIN_REVIEW_VERTICAL_SQUARE",
            "payloads": "{{ ti.xcom_pull(task_ids='prepare_period_report_payloads').get('payloads', []) }}"
        }
    )

    end = EmptyOperator(task_id="end")

    # 의존성 설정
    start >> load_reviews >> prepare_data >> [prepare_analysis, prepare_product_report, prepare_brand_report, prepare_period_report]
    prepare_analysis >> trigger_analysis
    prepare_product_report >> trigger_product_report
    prepare_brand_report >> trigger_brand_report
    prepare_period_report >> trigger_period_report
    [trigger_analysis, trigger_product_report, trigger_brand_report, trigger_period_report] >> end
