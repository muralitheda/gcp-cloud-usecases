
# Cloud Modernization: Public Cloud (SAAS Usecase) - Fully Managed (Serverless) DWH Usecase 9: BigQuery End-to-End Pipeline Building

This document outlines a fully managed, serverless Data Warehouse (DWH) pipeline built on Google BigQuery, designed for robust data ingestion, transformation, analysis, and consumption in a public cloud (SaaS) context.

## Architecture Overview
<details>
  The pipeline consists of several distinct layers, each serving a specific purpose in the data lifecycle:
  <summary> Click to view the E2E diagram </summary>
  <img src="images/usecase9-dataflow-diagram.png" alt="E2E Diagram">
</details>

### 1. Ingestion/RAW Layer

This is the entry point for raw transactional data. All raw data is ingested into BigQuery.

* **POS Trans Data (CSV Load):**
    * Manual Table Creation & CSV Load into BigQuery.
    * Undergoes processes for Data Discovery, Quality, Masking & Classification, and Views creation.
    * Managed by **Data Stewards**.
* **Online Trans Data (JSON Load):**
    * JSON files are loaded with auto-detection of schema into BigQuery.
* **Mobile Trans Data (CSV Load):**
    * CSV files are loaded with auto-detection of schema, including column names, into BigQuery.
* **Raw Data Analytics:**
    * A dedicated path for **Data Analysts** to directly work with raw data.
* **BigQuery (Overwrite (EL)) Load Command:**
    * Handles `CSV load` with `Skip header` for `Consumer Data`.

### 2. Curated Layer

This layer transforms raw data into a clean, structured, and curated format suitable for analysis.

* **BigQuery ETL (Curation):**
    * Processes include Data Discovery, Data Munging, Data Customization, Data Curation.
    * Supports Incremental/Complete Refresh strategies.

### 3. Analytical Layer

This layer prepares the curated data for various analytical and consumption purposes, focusing on business insights.

* **BigQuery Analytical Processing:**
    * Performs Denormalization, Data Wrangling (join), Aggregation, Consolidation.
    * Calculates Key Performance Indicators (KPI) & Metrics.
    * Enables Analytical & Windowing functions.

### 4. Consumption Layer

The final layer where processed data is consumed by various stakeholders and downstream systems.

* **Visual Analytics & Dashboard:**
    * Utilizes tools like Looker for Visualization & Dashboarding for **Clients & Business Team**.
    * Connects directly to the Analytical Layer.
* **AIML (Artificial Intelligence/Machine Learning):**
    * **Data Scientists** consume data from the Analytical Layer for AI/ML model development and training.
* **Decision Support Systems (KPI & Metrics):**
    * Provides actionable insights for **Clients & Business Team** based on calculated KPIs and metrics.
* **File Export:**
    * Enables data export to `Egress/Downstream Systems/Outbound Feeds`.

## Key Stakeholders

* **Data Stewards:** Responsible for data discovery, quality, masking, classification, and view creation in the RAW Layer.
* **Data Analysts:** Utilize the RAW Layer for immediate raw data analytics.
* **Data Scientists:** Consume data from the Analytical Layer for AI/ML initiatives.
* **Clients & Business Team:** Primary consumers of insights via dashboards, decision support systems, and potentially direct data access.

## Technology Stack

* **Core Data Warehouse:** Google BigQuery (Serverless, Fully Managed)
* **Visualization & Dashboarding:** Looker
* **Data Ingestion:** CSV, JSON (with auto-schema detection capabilities)

**Prerequisties:**
```bash
gcloud auth login
```
**Ensure to copy the code into codebase bucket and custs data**
```bash
#Use your local PC/VM and make sure gcloud is already installed
cd ~/Downloads/ 
git clone https://github.com/muralitheda/gcp-cloud-usecases.git #copy his repo url from github  

gsutil cp /home/hduser/Downloads/gcp-cloud-usecases/usecase9-modernization4-gcp-bigquery-serverless/usecase1_d_consumer_bq_Outbound_data_load.sql gs://iz-cloud-training-project-bucket/codebase/
gsutil cp /home/hduser/Downloads/gcp-cloud-usecases/usecase9-modernization4-gcp-bigquery-serverless/usecase1_consumer_bq_raw_load.sql gs://iz-cloud-training-project-bucket/codebase/
gsutil cp /home/hduser/Downloads/gcp-cloud-usecases/usecase9-modernization4-gcp-bigquery-serverless/usecase1_c_consumer_bq_discovery_load.sql gs://iz-cloud-training-project-bucket/codebase/
gsutil cp /home/hduser/Downloads/gcp-cloud-usecases/usecase9-modernization4-gcp-bigquery-serverless/usecase1_b_consumer_bq_curation_load.sql gs://iz-cloud-training-project-bucket/codebase/

gsutil cp /home/hduser/Downloads/gcp-cloud-usecases/usecase9-modernization4-gcp-bigquery-serverless/dataset/*.* gs://iz-cloud-training-project-bucket/datasets/

#dataset verification : custs
gsutil ls gs://iz-cloud-training-project-bucket/dataset/
```


## Step1 :: Detail: Loading the RAW (Bronze) Layer
This section details the initial loading process for the RAW Layer (also referred to as the Bronze Layer) using different BigQuery load commands based on data type.

<details>
  <summary>Click here to view flow diagram</summary>
    <img src="images/usecase9_step1.png" alt="E2E Diagram">
</details>

Either in the BQ Console or using bq command run in Cloud shell use the below query to create a raw tables
```bash
cd ~/Downloads
gsutil cp gs://iz-cloud-training-project-bucket/codebase/usecase1_consumer_bq_raw_load.sql ~/Downloads/
bq query --use_legacy_sql=false < usecase1_consumer_bq_raw_load.sql
```
## Step 2

## Step 3

## Step 4