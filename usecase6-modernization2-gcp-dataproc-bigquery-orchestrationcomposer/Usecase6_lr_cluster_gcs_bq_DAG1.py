import os
import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

PROJECT_ID = "iz-cloud-training-project"
CLUSTER_NAME="singlenode-cluster-dataproc-1"
REGION = "us-central1"
ZONE = "us-central1-a"
PYSPARK_CODE1_URI = "gs://iz-cloud-training-project-bucket/codebase/code_Usecase6_step1_gcs_bq.py"#this code is kept in your bucket location

default_args = {"start_date": days_ago(1),"project_id": PROJECT_ID,}

with models.DAG(
    "DAG-Usecase-6-To-Submit-PySpark-Task-LR-Cluster",
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),  
) as dag:
   
   pyspark_job_params = {
   "reference": {"project_id": PROJECT_ID},
   "placement": {"cluster_name": CLUSTER_NAME},
   "pyspark_job": {"main_python_file_uri": PYSPARK_CODE1_URI},}
   
   pyspark_task = DataprocSubmitJobOperator(task_id="pyspark_task1", job=pyspark_job_params, region=REGION, project_id=PROJECT_ID)