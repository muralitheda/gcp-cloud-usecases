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

1. **Step1**
