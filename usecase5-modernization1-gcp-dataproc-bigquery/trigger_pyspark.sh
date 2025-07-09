#!/bin/bash
gcloud dataproc jobs submit pyspark --cluster=singlenode-cluster-dataproc-1 --region=us-central1 gs://iz-cloud-training-project-bucket/codebase/Usecase5_gcsToBQRawToBQCurated.py
if [ $? -ne 0 ]
then
echo "`date` error occured in the Pyspark job" > /tmp/gcp_pyspark_schedule.log
else
echo "`date` Pyspark job is completed successfully" > /tmp/gcp_pyspark_schedule.log
fi
echo "`date` gcloud pyspark ETL script is completed" >> /tmp/gcp_pyspark_schedule.log