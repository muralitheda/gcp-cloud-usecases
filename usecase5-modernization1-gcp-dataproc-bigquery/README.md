# Use Case #5: Cloud Modernization 1 - Public Cloud (SaaS Use Case) Dataproc - BigQuery

This README outlines a cloud modernization use case leveraging Google Cloud Platform (GCP) services for ETL (Extract, Transform, Load) and analytics. The core idea is to move from on-premises solutions (like Hive for analytics) to cloud-native, managed services, specifically using Dataproc for Spark-based ETL and BigQuery for powerful, scalable analytics.

---

## 1. Overview and Objective

This use case demonstrates a cloud modernization strategy where **Spark code runs on a Dataproc cluster for ETL processes**, and the **managed BigQuery service is utilized for analytics**. This approach is preferred over traditional on-premises Hive for analytics due to BigQuery's advanced features, scalability, and flexibility, making it an ideal choice for data analysts, data scientists, and data engineers. This strategy emphasizes a "cloud-agnostic" mindset by focusing on managed services.


## 2. Architecture Overview

The solution involves:

* **Google Cloud Storage (GCS):** Used as the primary data lake for raw customer data (`Cust Data`) and intermediate curated layers.

* **Dataproc LR Cluster:** A long-running Dataproc cluster where Spark SQL jobs execute the ETL logic.

* **Google BigQuery:** Serves as the analytical data warehouse, with distinct datasets for `Raw layer` and `Curated Layer`.

* **Google Data Studio:** Used for data visualization and reporting on top of BigQuery.

* **Cron Scheduler:** Orchestrates and schedules the Spark job submissions for recurring execution.

The flow involves data moving from GCS to Dataproc for ETL, then into BigQuery (Raw and Curated layers), and finally consumed by Data Studio.


## 3. Key Learnings from this Use Case

Building upon previous use cases, this scenario fundamentally focuses on achieving complete cloud migration by:

* **Eliminating On-Premise Dependencies:** Moving away from on-premises infrastructure entirely.

* **Spark for ETL Only:** Utilizing Dataproc Spark primarily for ETL operations, *not* for analytics.

* **BigQuery for Analytics:** Emphasizing BigQuery as the dedicated analytical engine, replacing traditional Hive for this purpose.

* **Cluster Choice:** Highlighting the suitability of Long-Running (LR) or Ephemeral (EPH) Dataproc clusters based on specific ETL workload patterns.


## 4. Steps to Execute the Use Case

Follow these steps to set up and run the Spark ETL process with BigQuery on GCP:

**Prerequisties:**

```bash
gcloud auth login
```

1. **Once for all - Create the Long running cluster (logging into the edge node of the onprem cluster):**

```bash
gcloud dataproc clusters create singlenode-cluster-dataproc-1 --region us-central1 --zone us-central1-a --enable-component-gateway --single-node --master-machine-type e2-standard-2 --master-boot-disk-size 100 --image-version 2.1-debian11 --project iz-cloud-training-project --max-idle 7200s
```
```bash
#Check cluster running status
gcloud dataproc clusters describe singlenode-cluster-dataproc-1 --region=us-central1
```
```bash
#Connect to cluster master/edge node
gcloud compute ssh --zone "us-central1-a" "singlenode-cluster-dataproc-1-m" --project "iz-cloud-training-project"
```

2. **Create BQ datasets (Equivalent to Database in hive) – Cloud cli/Edgenode(linux/windows)/Console**

```bash
#Creating BigQuery Dataset
bq mk rawds
bq mk curatedds

#Note: Either in the BQ Console/cli use the below query to create a raw (native) table
bq query --use_legacy_sql=false "create or replace table rawds.customer_raw(custno INT64, firstname STRING,lastname STRING,age INT64,profession STRING);"
#spark will create this table if we don’t create it…
```

3. **Ensure to copy the code and custs data**
```bash
#sudo yum install git  
git config --global user.name "muralitheda"  
git config --global user.email "yourmailaddress@dot.com"  
git config --list  
git init  
cd .git/  
git clone https://github.com/muralitheda/gcp-cloud-usecases.git #copy his repo url from github  

gsutil cp /home/hduser/.git/gcp-cloud-usecases/usecase5-modernization1-gcp-dataproc-bigquery/Usecase5_gcsToBQRawToBQCurated.py gs://iz-cloud-training-project-bucket/codebase/
gsutil cp /home/hduser/.git/gcp-cloud-usecases/usecase5-modernization1-gcp-dataproc-bigquery/trigger_pyspark.sh gs://iz-cloud-training-project-bucket/codebase/

#dataset verification : custs
gsutil cat gs://iz-cloud-training-project-bucket/custs | head -n 5

```

4. **PySpark Code**

```bash
vi /home/hduser/.git/gcp-cloud-usecases/usecase5-modernization1-gcp-dataproc-bigquery/Usecase5_gcsToBQRawToBQCurated.py
```
```python
#prerequisites
#gcloud dataproc jobs submit pyspark --cluster=wd28cluster --region=us-east1 --jars gs://com-inceptez-data/jars/spark-3.1-bigquery-0.27.1-preview.jar,gs://com-inceptez-data/jars/gcs-connector-latest-hadoop2.jar /home/hduser/install/gcp/gcsToBQRawToBQCurated.py
from pyspark.sql.functions import *
from pyspark.sql.types import *
def main():
   from pyspark.sql import SparkSession

   # define spark configuration object
   spark = SparkSession.builder\
      .appName("GCP GCS Read & Write to BigQuery") \
      .getOrCreate()
   spark.sparkContext.setLogLevel("ERROR")
   sc=spark.sparkContext
   conf = spark.sparkContext._jsc.hadoopConfiguration()
   conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
   conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

   print("-----------------------------------------------------------------------------------------------------------------------------------")
   print("Usecase5: Spark Application to Read from GCS and load to Bigquery (Raw) and load into another Bigquery (Curated) table in the GCP")
   print("-----------------------------------------------------------------------------------------------------------------------------------")

   print("[INFO] 1. Ingestion layer data read from GCS - brought by some data producers")
   gcs_df = spark.read.option("header", "false").option("delimiter", ",").option("inferschema", "true")\
   .csv("gs://iz-cloud-training-project-bucket/custs").toDF("custno","firstname","lastname","age","profession")
   gcs_df.show()
   print("[INFO] 2. GCS Read Completed Successfully")
   print("[INFO] Ensure to create the BQ datasets (rawds & curatedds) -> table creation (optional) ")

   print("[INFO] 3. Writing GCS data to Raw BQ table")
   # We need to set the below two properties to enable the inline view queries and dataset info
   #spark.conf.set("viewsEnabled","true")
   #spark.conf.set("materializationDataset","rawds")
   #sourcesystem data -> pyspark DF -> write to temporaryGcsBucket -> read from GCS using a viewsEnabled -> store the final result materializationDataset -> 'rawds.customer_raw'
   #db -> sqoop import -> hive (sqoop imports data from db and store the interiem data into HDFS (temp loc) -> hive table (load data inpath)
   # We need to set the below properties to enable the temp GCS location (change to your GCS location) for bq write

   gcs_df.write.mode("overwrite").format('com.google.cloud.spark.bigquery.BigQueryRelationProvider') \
   .option("temporaryGcsBucket",'iz-cloud-training-project-bucket/tmp')\
   .option('table', 'rawds.customer_raw') \
   .save()
   print("[INFO] 4. GCS to Raw BQ raw table loaded")

   #Execute the below steps if we have a seperate spark pipeline running to read data from BQ raw to the BQ curated 
   # gcs -> bqRawwrite -> bqRawread -> bqCuratedwrite
   #(provided if the raw ingestion is managed by ingestion team and curation is taken care by curation team)
   #print("Reading data from raw table and writing to BQ table in case if we create it as a seperate pipeline")
   #sql = """select custno, concat(firstname,",", lastname) as name, age, coalesce(profession,"unknown") as profession from rawds.customer_raw where age>30""" #pushdown optimization
   #print("raw BQ table to Curated BQ table load completed")
   #df = spark.read.format("bigquery").load(sql)
   #df.write.mode("overwrite").format('bigquery').option("temporaryGcsBucket",'incpetez-data-samples/tmp').option('table', 'curatedds.customer_curated').save()
   # gcs -> bqRawwrite 
   #     -> bqCuratedwrite

   gcs_df.createOrReplaceTempView("raw_view")
   curated_bq_df=spark.sql("select custno, concat(firstname,',', lastname) as name, age, coalesce(profession,'unknown') as profession from raw_view where age>30")
   print("[INFO] 5. Read from rawds is completed")

   # We need to set the below propertie to enable the temp GCS location for bq write
   curated_bq_df.write.mode("overwrite").format('com.google.cloud.spark.bigquery.BigQueryRelationProvider') \
   .option("temporaryGcsBucket",'iz-cloud-training-project-bucket/tmp')\
   .option('table', 'curatedds.customer_curated') \
   .save()
   print("[INFO] 6. GCS to Curated BQ table load completed")

main()
```

5. **Before run the gcloud pyspark job, ensure the code, dataset is copied, BQ datasets are created.**  
```bash
gcloud dataproc jobs submit pyspark --cluster=singlenode-cluster-dataproc-1 --region=us-central1 gs://iz-cloud-training-project-bucket/codebase/Usecase5_gcsToBQRawToBQCurated.py
```

6. **Check for the data load status**  
```bash
bq query --use_legacy_sql=false "select * from rawds.customer_raw limit 10"
bq query --use_legacy_sql=false "select * from curatedds.customer_curated limit 10"
```

7. **Schedule the above spark job submission to run in a scheduled interval (cron scheduler)**
```bash
vi trigger_pyspark.sh
```

```bash
#!/bin/bash
gcloud dataproc jobs submit pyspark --cluster=singlenode-cluster-dataproc-1 --region=us-central1 gs://iz-cloud-training-project-bucket/codebase/Usecase5_gcsToBQRawToBQCurated.py
if [ $? -ne 0 ]
then
echo "`date` error occured in the Pyspark job" > /tmp/gcp_pyspark_schedule.log
else
echo "`date` Pyspark job is completed successfully" > /tmp/gcp_pyspark_schedule.log
fi
echo "`date` gcloud pyspark ETL script is completed" >> /tmp/gcp_pyspark_schedule.log
```

```bash
gsutil cp gs://iz-cloud-training-project-bucket/codebase/trigger_pyspark.sh /home/hduser/

chmod 777 /home/hduser/trigger_pyspark.sh

crontab -e
*/2 * * * * bash /home/hduser/trigger_pyspark.sh

```

