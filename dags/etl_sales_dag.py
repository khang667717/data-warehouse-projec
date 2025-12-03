from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import sys
import os

# Add scripts directory to path
sys.path.insert(0, '/opt/airflow/scripts')

from scripts.etl_pipeline import ETLPipeline

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

def run_full_etl():
    """Run full ETL pipeline"""
    pipeline = ETLPipeline()
    pipeline.run_full_pipeline()

def run_incremental_etl():
    """Run incremental ETL"""
    pipeline = ETLPipeline()
    pipeline.run_incremental()

def validate_etl():
    """Validate ETL results"""
    pipeline = ETLPipeline()
    results = pipeline.validate_results()
    
    # Log results
    for key, value in results.items():
        print(f"{key}: {value}")

with DAG(
    dag_id='sales_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for sales data warehouse',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['sales', 'etl', 'data-warehouse']
) as dag:
    
    # Task 1: Check database connection
    check_db = MySqlOperator(
        task_id='check_database_connection',
        mysql_conn_id='mysql_dw',
        sql='SELECT 1'
    )
    
    # Task 2: Create tables if not exists
    create_tables = BashOperator(
        task_id='create_tables',
        bash_command='python /opt/airflow/scripts/create_tables.py'
    )
    
    # Task 3: Run full ETL
    run_etl = PythonOperator(
        task_id='run_full_etl',
        python_callable=run_full_etl
    )
    
    # Task 4: Validate results
    validate_results = PythonOperator(
        task_id='validate_etl_results',
        python_callable=validate_etl
    )
    
    # Task 5: Update metadata
    update_metadata = MySqlOperator(
        task_id='update_etl_metadata',
        mysql_conn_id='mysql_dw',
        sql="""
            INSERT INTO etl_metadata 
            (process_name, start_time, end_time, status)
            VALUES ('AIRFLOW_DAG', '{{ ds }}', NOW(), 'COMPLETED')
        """
    )
    
    # Task 6: Send notification (placeholder)
    send_notification = BashOperator(
        task_id='send_notification',
        bash_command='echo "ETL pipeline completed successfully on {{ ds }}"'
    )
    
    # Define task dependencies
    check_db >> create_tables >> run_etl >> validate_results >> update_metadata >> send_notification