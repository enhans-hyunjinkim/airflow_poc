from __future__ import annotations

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow import DAG

from operators.s3_data_processor_operator import S3NaverReviewProcessorOperator
from operators.data_validator_operator import ReviewDataValidatorOperator
from operators.mongo_operator import MongoUpsertOperator

MONGO_CONN_ID = "mongo_agent_ground"
AWS_CONN_ID = "aws_default"
SLACK_CONN_ID = "slack_default"


# ========== 데이터 준비 함수들 ==========

def update_latest_product_info(**context):
    """최신 상품 정보 업데이트"""
    ti = context["ti"]

    # process_s3_data에서 product_ids 추출
    review_data = ti.xcom_pull(task_ids="process_s3_data") or []
    product_ids = list(set([doc.get("product_id") for doc in review_data if doc.get("product_id")]))

    if not product_ids:
        return {"updated": 0}

    from airflow.providers.mongo.hooks.mongo import MongoHook

    mongo_hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    coll = mongo_hook.get_collection("woongjin__product_review_analysis")

    updated_count = 0
    for pid in product_ids:
        cursor = coll.find(
            {"product_id": pid, "product_name": {"$exists": True}, "category_id": {"$exists": True}},
            sort=[("created_at", -1)],
            limit=1
        )
        latest = next(cursor, None)
        if not latest:
            continue

        update_fields = {}
        if latest.get("product_name"):
            update_fields["product_name"] = latest["product_name"]
        if latest.get("category_id"):
            update_fields["category_id"] = latest["category_id"]

        if update_fields:
            res = coll.update_many({"product_id": pid}, {"$set": update_fields})
            updated_count += res.modified_count

    return {"updated": updated_count}




# ========== DAG 정의 ==========
default_args = {"owner": "data-connector", "depends_on_past": False}

with DAG(
        "woongjin_naver_review_dag",
        default_args=default_args,
        description="Woongjin Naver Smart Store Review Dag",
        schedule="30 17 * * *",
        catchup=False,
        tags=["woongjin", "naver-review"],
        render_template_as_native_obj=True,
        params={
            "bucket_name": "enhans-collectify-chrome-extention-prod"
        },
) as dag:

    start = EmptyOperator(task_id="start")

    # S3 파일 목록 조회
    list_s3_files = S3ListOperator(
        task_id="list_s3_files",
        aws_conn_id=AWS_CONN_ID,
        bucket="{{ params.bucket_name }}",
        prefix="woongjin/review/NAVER_SMART_STORE/{{ macros.ds_add(ds, -1).replace('-', '/') }}/",
        do_xcom_push=True,
    )

    # S3 데이터 처리
    process_s3_data = S3NaverReviewProcessorOperator(
        task_id="process_s3_data",
        aws_conn_id=AWS_CONN_ID,
        bucket_name="{{ params.bucket_name }}",
        s3_keys="{{ ti.xcom_pull(task_ids='list_s3_files') }}",
        output_format='json'
    )

    # MongoDB에 저장 (process_s3_data에서 직접 데이터 받기)
    save_to_mongo = MongoUpsertOperator(
        task_id="save_to_mongo",
        conn_id=MONGO_CONN_ID,
        collection="woongjin__product_review_analysis",
        documents="{{ ti.xcom_pull(task_ids='process_s3_data') }}",
        filter_fields=["review_id"],
        many=True
    )

    # 최신 상품 정보 업데이트
    update_latest = PythonOperator(
        task_id="update_latest_product_info",
        python_callable=update_latest_product_info
    )

    # 데이터 검증
    validate_data = ReviewDataValidatorOperator(
        task_id="validate_today_data",
        conn_id=MONGO_CONN_ID,
        collection="woongjin__product_review_analysis",
        target_date="{{ macros.ds_add(ds, -1) }}",
        date_field="created_day",
        min_documents=1,
        check_sentiment=False
    )

    # Slack 알림
    slack_alert = SlackWebhookOperator(
        task_id="slack_failure_alert",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=(
            ":rotating_light: *Woongjin 리뷰 데이터 수집 실패*\n"
            "*대상 날짜*: `{{ macros.ds_add(ds, -1) }}`\n"
            "해당 날짜의 리뷰 데이터 수집에 실패했습니다."
        ),
        channel="#data-alerts",
        trigger_rule="one_failed",
    )

    end = EmptyOperator(task_id="end")

    # 의존성 설정
    start >> list_s3_files >> process_s3_data >> save_to_mongo >> update_latest >> validate_data
    validate_data >> slack_alert
    validate_data >> end