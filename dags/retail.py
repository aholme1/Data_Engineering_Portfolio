from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.bash import BashOperator
from astro import sql as aql
from astro.files import File
from airflow.models.baseoperator import chain
from astro.sql.table import Table, Metadata
from astro.constants import FileType
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

@dag(
    dag_id='retail',  # Matching the dag_id you're trying to test
    start_date=datetime(2023, 1, 1),
    schedule='0 0 * * *',  # Runs at midnight every day (in UTC)
    catchup=False,
    tags=['retail']
)

def retail_dag():
    
    # Due to permission errors, dbt code must be coppied to a seperate directory
    copy_dbt = BashOperator(
        task_id='copy_dbt',
        bash_command='cp -r /usr/local/airflow/include/dbt /usr/local/airflow'
    )
    
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='/usr/local/airflow/include/dataset/online_retail.csv',
        dst='raw/data.csv',
        bucket='aholme1_sales_storage',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )
    
    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='retail',
        gcp_conn_id='gcp'
    )
    
    gcs_to_raw = aql.load_file(
        task_id='gcs_to_raw',
        input_file=File(
            'gs://aholme1_sales_storage/raw/data.csv',
            conn_id='gcp',
            filetype=FileType.CSV
        ),
        output_table=Table(
            name='raw_invoices',
            conn_id='gcp',
            metadata=Metadata(schema='retail')
        ),
        use_native_support=False,
    )
    
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python3')
    # Use python env for soda data check
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)

    
    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )
    
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python3')
    # Use python env for soda data check
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)

    
    report = DbtTaskGroup(
        group_id='report',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )
    
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python3')
    # Use python env for soda data check
    def check_report(scan_name='check_report', checks_subpath='reports'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    
    chain(
        copy_dbt,
        upload_csv_to_gcs,
        create_retail_dataset,
        gcs_to_raw,
        check_load(),
        transform,
        check_transform(),
        report,
        check_report(),
    )
    
retail_dag()