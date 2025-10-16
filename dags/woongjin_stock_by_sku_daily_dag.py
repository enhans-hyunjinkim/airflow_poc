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
        "woongjin_stock_by_sku_daily_dag",
        default_args=default_args,
        description="Woongjin Stock by SKU Daily DAG",
        schedule='0 3 * * *', # 10:00 KST
        catchup=False,
        tags=["stock", "woongjin"],
        render_template_as_native_obj=True,
) as dag:

    start_task = EmptyOperator(task_id="start_task")

    # List of offsets: 0 = today, 1 = yesterday, 2 = 2 days ago
    offsets = [0, 1, 2]

    # Download tasks: one per offset
    download_stock_data = StockDownloadOperator.partial(
        task_id='download_stock_data',
        company_str='wj-kpiapi',
        type='sku_daily',
        log_response=False
    ).expand(
        data=[
            {
                "date": "{{ macros.ds_add(ds, -%d) }}" % off,
                "day": "{{ (macros.ds_add(ds, -%d))[8:] }}" % off
            }
            for off in offsets
        ]
    )

    # Transform mapped downloads
    transform_stock_data = StockJsonToMongoOperator(
        task_id='transform_stock_data',
        source_task_id="download_stock_data",
        collection_name='woongjin__stock_by_sku_daily'
    )


    # Load mapped transforms into Mongo
    load_stock_data = MongoUpsertOperator(
        task_id='load_stock_data',
        conn_id='mongo_agent_ground',
        collection='woongjin__stock_by_sku_daily',
        documents="{{ ti.xcom_pull(task_ids='transform_stock_data') }}",
        filter_fields=['sku_id', '_date'],
        many=True
    )

    end_task = EmptyOperator(task_id="end_task")

    # Define dependencies
    start_task >> download_stock_data >> transform_stock_data >> load_stock_data >> end_task