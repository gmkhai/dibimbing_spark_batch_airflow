import yaml
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with open('dags/configs/list_table_assignment.yml') as f:
    file_config = yaml.safe_load(f)

@dag(
    dag_id='dag-assigment-etl-spark',
    schedule_interval='@daily',
    description="Assignment ETL Spark",
    start_date=days_ago(1),
)
def dag_assignment_etl_spark():
    start_task = EmptyOperator(
        task_id='start_task'
    )

    etl_task = []
    for etl in file_config.get('etls'):
        etl_process = SparkSubmitOperator(
            task_id=f"etl_{etl.get('table')}",
            application=etl.get('path'),
            conn_id="spark_main",
            jars='/spark-scripts/jars/jars_postgresql-42.2.20.jar'
        )

        etl_task.append(etl_process)

    analysis_task = SparkSubmitOperator(
        task_id='analysis_skenario_olist',
        application='/spark-scripts/analysis/assignment_analysis_usecase.py',
        conn_id="spark_main",
        jars='/spark-scripts/jars/jars_postgresql-42.2.20.jar'
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    start_task >> etl_task >> analysis_task >> end_task

dag_assignment_etl_spark()