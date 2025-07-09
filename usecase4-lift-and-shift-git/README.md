# Hybrid Cloud (SAAS) Use Case: Long Running Cluster

This document outlines a specific use case (Use Case 4) for a hybrid cloud SAAS solution, focusing on a long-running cluster environment. It also touches upon the underlying GCP Cloud architecture and relevant interview preparation points.

## Use Case 4: Overview and Steps

This use case describes the process for deploying and scheduling applications in a production environment, leveraging Git and Google Cloud Platform (GCP) services.

1.  **Developer Developed & Pushed Code to Git:** The development team initiates the process by developing the necessary code and pushing it to a Git repository.
2.  **Login to Master Node (Public DNS) using PuTTY:** Production Deployment Team/Data Engineers access the production environment by logging into the master node using its public DNS via PuTTY.
3.  **Install / Configure Git (Admin will do once for all):** An administrator is responsible for the one-time installation and configuration of Git on the necessary systems.
4.  **Login to Master Node & Clone Git Repo:** The Production Deployment Team logs into the master node and clones the Git repository to download the code required for production.
5.  **Create a Shell Script and Submit PySpark Job using gcloud command:** A shell script is created to submit the PySpark job. This job is then initiated using the `gcloud` command, interacting with Google Cloud services.
6.  **Schedule the Above Script on On-Prem Edge Node using Cron Tab:** The shell script, which triggers the PySpark job, is scheduled to run periodically using a cron job on an on-premise edge node.

## Interview Preparation Focus

This use case provides excellent material for answering interview questions related to:

* **Migration of Hive and Spark from On-Prem to Cloud:** Demonstrates a practical example of how these technologies can be lifted and shifted to a cloud environment.
* **Transitioning from On-Premise Dependency:** Explains how to move away from a completely on-premise infrastructure.
* **Utilizing Spark for ETL vs. Hive:** Discusses the advantages and use cases of Spark for Extract, Transform, Load (ETL) processes compared to traditional Hive.
* **Leveraging Git for Version Control:** Highlights the importance and application of Git in a cloud deployment pipeline.
* **Scheduling Jobs with Google Cloud Provided Cron Scheduler:** Focuses on the use of Google Cloud's scheduling capabilities for automating tasks.


## Workflow

1.  **Manual Code Cloning/Pull (via GitHub):**
    * **GitHub (Version Control & Code Repo):** Developers commit their code here.
    * **Clone Code (to DP Master Node):** Code is cloned from GitHub to the Dataproc Master Node.
2.  **Code Check-in/Commit (via GitHub):** Developers push their changes back to GitHub.
3.  **Developer Workflow (On-Premise):**
    * **Windows/Edge Node:** Developers develop PySpark code and commit it to GitHub.
    * **Shell Script -> Cron Scheduled:** A shell script (running on the edge node) is scheduled via Cron to trigger jobs.
4.  **Data Flow within GCP:**
    * **ETL:** Data is processed through ETL pipelines.
    * **Analyze:** Processed data is analyzed using various tools.
5.  **Outbound Systems:** Results and processed data can be sent to external or "outbound" systems.

## Business Impact & Outcomes (Health Tracking Example)

The solution enables advanced analytics and real-time insights, exemplified by its application in health tracking:

* Predictive Analytics
* Fraud & Abuse Prevention
* Improved Diagnostics

## Execution Steps

**Prerequisties:**

```bash
gcloud auth login
```

**1. Admin - Once for all - Create a long running dataproc cluster**

```bash
gcloud dataproc clusters create cluster-dataproc-2 --enable-component-gateway --bucket iz-dataproc-uscentral1-bucket-1 --region us-central1 --zone us-central1-a --master-machine-type e2-standard-2 --master-boot-disk-size 100 --num-workers 3 --worker-machine-type e2-standard-2 --worker-boot-disk-size 100 --image-version 2.1-rocky8 --properties hdfs:dfs.blocksize=268435456 --max-idle 7200s --project iz-cloud-training-project 

gcloud dataproc clusters describe cluster-dataproc-2 --region=us-central1
```

**2. Admin - Once for all - Open the Dataproc Master node ssh (edge node of the cloud cluster) and execute the below steps:**

```bash
gcloud compute ssh --zone "us-central1-a" "cluster-dataproc-2-m" --project "iz-cloud-training-project"  
```

**3. Develop the pyspark code & pushed the code (Usecase4_GcpGcsReadWritehive_cloud.py) to GIT**  

https://github.com/muralitheda/gcp-cloud-usecases/blob/master/usecase4-lift-and-shift-git/Usecase4_GcpGcsReadWritehive_cloud.py  

**4.  Login to the dataproc master node using public dns or master node ssh or using vm edge node terminal and install the Git (Admin will do once for all)**  

```bash
#sudo yum install git  
git config --global user.name "muralitheda"  
git config --global user.email "yourmailaddress@dot.com"  
git config --list  
git init  
cd .git/  
```
**5. Login to master node & Clone the git repo to download the code (Production Deployment Team/CICD Tool)**  

```bash
git clone https://github.com/muralitheda/gcp-cloud-usecases.git #copy his repo url from github  
mkdir -p ~/project  
cp /home/muralisalaipudur/.git/gcp-cloud-usecases/usecase4-lift-and-shift-git/Usecase4_GcpGcsReadWritehive_cloud.py ~/project  
cp /home/muralisalaipudur/.git/gcp-cloud-usecases/usecase4-lift-and-shift-git/gcp_pyspark_yarn_client_schedule.sh ~/project  
chmod 777 ~/project/*  
```
**Ensure to copy the custs data from the gcp bucket/other location to the edge node**  

```bash
mkdir ~/dataset  
gsutil cp gs://iz-cloud-training-project-bucket/custs ~/dataset/    
```
**6. Create a shell script and submit the pyspark job using the gcloud command**
```bash
bash /home/muralisalaipudur/project/gcp_pyspark_yarn_client_schedule.sh  
```

**vi /home/muralisalaipudur/project/gcp_pyspark_yarn_client_schedule.sh**

```bash
#!/bin/bash
gcloud dataproc jobs submit pyspark --cluster=cluster-dataproc-2 --region=us-central1 --properties="spark.driver.memory=2g","spark.executor.memory=2g","spark.executor.instances=4","spark.executor.cores=2","spark.submit.deployMode=client","spark.sql.shuffle.partitions=10","spark.shuffle.spill.compress=true" /home/muralisalaipudur/.git/gcp-cloud-usecases/usecase4-lift-and-shift-git/Usecase4_GcpGcsReadWritehive_cloud.py  
if [ $? -ne 0 ]  
then  
echo "`date` error occured in the Pyspark job" > /tmp/gcp_pyspark_schedule.log  
else  
echo "`date` Pyspark job is completed successfully" > /tmp/gcp_pyspark_schedule.log  
fi  
echo "`date` gcloud pyspark ETL script is completed" >> /tmp/gcp_pyspark_schedule.log  
```

**vi /home/muralisalaipudur/project/Usecase4_GcpGcsReadWritehive_cloud.py**    

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    from pyspark.sql import SparkSession
    # define spark configuration object
    spark = SparkSession.builder\
       .appName("GCP GCS Hive Read/Write") \
       .enableHiveSupport()\
       .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    print("[INFO] Use Spark Application to Read csv data from cloud GCS and get a DF created with the GCS data in the on prem, "
          "convert csv to json in the on prem DF and store the json into new cloud GCS location")
    print("[INFO] Hive to GCS to hive starts here")
    custstructtype1 = StructType([StructField("id", IntegerType(), False),
                                  StructField("custfname", StringType(), False),
                                  StructField("custlname", StringType(), True),
                                  StructField("custage", ShortType(), True),
                                  StructField("custprofession", StringType(), True)])
    gcs_df = spark.read.csv("gs://iz-cloud-training-project-bucket/custs", mode='dropmalformed', schema=custstructtype1)
    gcs_df.show(10)
    print("[INFO] GCS Read Completed Successfully")
    gcs_df.write.mode("overwrite").partitionBy("custage").saveAsTable("default.cust_info_gcs")
    print("[INFO] GCS to hive table load Completed Successfully")

    print("[INFO] Hive to GCS usecase starts here")
    gcs_df = spark.read.table("default.cust_info_gcs")
    curts = spark.createDataFrame([1], IntegerType()).withColumn("curts", current_timestamp()).select(date_format(col("curts"), "yyyyMMddHHmmSS")).first()[0]
    print("[INFO] ", curts)
    gcs_df.repartition(2).write.json("gs://iz-cloud-training-project-bucket/usecase4/cust_output_json_" + curts)
    print("[INFO] gcs Write Completed Successfully")

    print("[INFO] Hive to GCS usecase starts here")
    gcs_df = spark.read.table("default.cust_info_gcs")
    curts = spark.createDataFrame([1], IntegerType()).withColumn("curts", current_timestamp()).select(date_format(col("curts"), "yyyyMMddHHmmSS")).first()[0]
    print(curts)
    gcs_df.repartition(2).write.mode("overwrite").csv("gs://iz-cloud-training-project-bucket/usecase4/cust_csv")
    print("[INFO] gcs Write Completed Successfully")

main()
```

**7. Schedule the above script in the Dataproc Cloud (Master) edge node using the cron tab.**
```bash
crontab -e
*/5 * * * * bash /home/muralisalaipudur/project/gcp_pyspark_yarn_client_schedule.sh 
```

**8. Validate the data in cloud Hive environment**  
```bash
gcloud dataproc jobs submit hive --cluster=cluster-dataproc-2 --region us-central1 -e "SELECT * FROM cust_info_gcs where custage>30 limit 100"
```

**Tip #1. Stop the cluster** 
```bash
gcloud dataproc clusters stop cluster-dataproc-2 --region=us-central1
```

**Tip #2. Start the cluster** 
```bash
gcloud dataproc clusters start cluster-dataproc-2 --region=us-central1
```

**Tip #3. Delete the cluster** 
```bash
gcloud dataproc clusters delete cluster-dataproc-2 --region=us-central1
```


