from __future__ import annotations

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from operators.json_to_mongo_operator import StockJsonToMongoOperator
from operators.mongo_operator import MongoUpsertOperator
from operators.oauth2_api_operator import StockDownloadOperator


default_args = {
    "owner": "data-connector",
    "depends_on_past": False,
}

with DAG(
        "woongjin_stock_amt_dag",
        default_args=default_args,
        description="Woongjin Stock Amount DAG",
        schedule='0 1 * * *', # 10:00 KST
        catchup=False,
        tags=["stock", "woongjin"],
        render_template_as_native_obj=True,
) as dag:

    start_task = EmptyOperator(
        task_id="start_task",
    )

    download_stock_data = StockDownloadOperator(
        task_id='download_stock_data',
        company_str='wj-kpiapi',
        type='amt',
        data={
            'date': '{{ ds_nodash }}',
        },
        log_response=False,
    )

    transform_stock_data = StockJsonToMongoOperator(
        task_id='transform_stock_data',
        source_task_id='download_stock_data',
        collection_name='woongjin__stock_amount',
        exclude_fields=['inbound_amt', 'inbound_qty', 'outbound_amt']
    )

    load_stock_data = MongoUpsertOperator(
        task_id='load_stock_data',
        conn_id='mongo_agent_ground',
        collection='woongjin__stock_amount',
        documents="{{ ti.xcom_pull(task_ids='transform_stock_data') }}",
        filter_fields=['date'],
        many=True
    )

    end_task = EmptyOperator(
        task_id="end_task",
    )

    start_task >> download_stock_data >> transform_stock_data >> load_stock_data >> end_task
