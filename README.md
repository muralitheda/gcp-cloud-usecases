>  Credits & References: This project was coached by [Mr. Mohamed Irfan from Inceptez Technologies](https://github.com/mohamedirfan?tab=repositories).

This README outlines various use cases for Dataproc clusters, focusing on data migration, lifting & shifting existing workloads, and modernizing data pipelines for cost savings and optimization.

## Dataproc Clusters Use Cases

### Usecase 1 & 2 – Migration of Legacy System to Bigdata and Data Access

**Objective:** Understand different data migration strategies between on-premise and cloud data platforms using PySpark.

* Ability to Migrate Data between OnPrem Datalake to Cloud Datalake using PySpark.
* Ability to Migrate Data between OnPrem Lakehouse to Cloud Datalake using PySpark.
* Ability to Migrate Data between Cloud Datalake to Onprem Datalake using PySpark.
* Ability to Migrate Data between Cloud Datalake to Onprem Lakehouse using PySpark.

### Usecase 3 & 4 – Lifting & Shifting

**Objective:** Learn how to migrate and run existing on-premise data workloads (Hive/Spark) on Dataproc.

* This use case will help migrating Hive and Spark code from on-premise to the cloud.
* Understand why we are shifting to GCP Dataproc due to vendor lock-in/license period with Cloudera.
* Ability to form Dataproc Multinode LR Cluster in GCP using GUI & CLI.
* Ability to Lift & Shift Hive HQLs and PySpark code from OnPrem Cloudera to Cloud Dataproc LR Cluster.
* Ability to do Cron scheduling from OnPrem Edgenode or Dataproc Master node.
* Understand how to use Git to clone code into a Dataproc Prod Cluster node.

### Usecase 5 & 6 – Modernizing

**Objective:** Modernize ETL and analytics pipelines using Dataproc, BigQuery, and orchestration tools.

* Learned about Cloud Modernization by using Dataproc for ETL & BigQuery Hive (Cloud Native) for Analytics.
* Learned how to develop PySpark job to run in Dataproc LR cluster and read data from GCS, Transform & load to Bigquery layers.
* Learned how to Orchestrate & Schedule PySpark job into Dataproc Long running cluster using Cron & Composer (Airflow) fundamentally (Pay For Not Use!).

### Usecase 7 & 8 – Modernizing, Optimization & Cost Saving

**Objective:** Optimize and reduce costs for Spark workloads using Dataproc ephemeral and serverless clusters with advanced scheduling and autoscaling.

* Learned about Cloud Modernization & Cost Saving measures by scheduling PySpark in Dataproc Ephemeral Cluster using Composer (Airflow) (Pay For Use!).
* Learned about Cloud Modernization & Cost Saving measures by scheduling PySpark in Serverless Spark Cluster using Composer (Airflow) (Pay Per Use!).
* Learned how to enable and do Autoscaling in Dataproc (LR & EPH), and how Serverless will do Autoscaling on its own.


### Usecase 9 – Modernizing using GCP BigQuery E2E

**Objective:** To demonstrate and implement a full BigQuery data pipeline, loading raw data, then transforming and curating it into analytical layers using various loading methods and advanced SQL.

* Learned about Practical skills in BigQuery data loading.  
* Learned about advanced SQL for complex data transformation and curation (including procedural logic and optimization).  
* creating layered data architectures, and real-world data engineering tasks using Google Cloud's `gsutil` and `bq` commands.