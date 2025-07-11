from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteBatchOperator
from datetime import datetime

PROJECT_ID = "iz-cloud-training-project"
CLUSTER_NAME="singlenode-ondemand-cluster"
REGION = "us-central1"
ZONE = "us-central1-a"
PYSPARK_CODE1_URI = "gs://iz-cloud-training-project-bucket/codebase/code_Usecase6_step1_gcs_bq.py"
BIGQUERY_CONNECTOR_JAR="gs://spark-lib/bigquery/spark-3.1-bigquery-0.32.2.jar"

# Define default arguments
default_args = {
    'owner': 'airflow',
    "start_date": datetime(2025, 7, 8),
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1}

# Define the DAG
dag = DAG(
    'DAG-Usecase-8-To-Submit-PySpark-Task-Serverless-Spark-Cluster',
    default_args=default_args,
    description='Submit a serverless Spark job to Dataproc',
    schedule_interval="*/30 * * * *",
    tags=['iz-10mins-serverless'],
    catchup=True,
    max_active_runs=1,
)

# Define the configuration for the Dataproc batch job
batch_config = {
    "pyspark_batch": {
        "main_python_file_uri": PYSPARK_CODE1_URI,
		"jar_file_uris": [BIGQUERY_CONNECTOR_JAR]
    },
    "runtime_config": {
        "version": "1.1",
        "properties": {
            "spark.executor.cores": "4",
            "spark.driver.cores": "4",
            "spark.executor.instances": "2",
        },
    }
}

# Define the task to create the Dataproc batch job
create_batch = DataprocCreateBatchOperator(
    task_id='create_batch',
    project_id=PROJECT_ID,
    region=REGION,
    batch=batch_config,
    batch_id='iz-serveless-spark-batch4',
    dag=dag,
)

# Define the task to delete the Dataproc batch job after completion
delete_batch = DataprocDeleteBatchOperator(
    task_id='delete_batch',
    project_id=PROJECT_ID,
    region=REGION,
    batch_id='iz-serveless-spark-batch4',
    trigger_rule='all_done',
    dag=dag,
)

create_batch >> delete_batch