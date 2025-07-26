### 📌 **Cloud Modernization Use Case – Serverless BigQuery Pipeline with External Tables & Stored Procedures**
---

### ✅ **Objective**

This usecase demonstrates a **cloud-native, serverless data warehouse pipeline** built on **Google BigQuery**, leveraging **BigLake external tables** and **stored procedures** for end-to-end data processing, analytics, and business intelligence.

## Architecture Overview
<details>
  <summary> Click to view the E2E diagram </summary>
  <img src="images/usecase10.png" alt="E2E Diagram">
</details>

---

### 🧩 **Pipeline Overview:**

#### **1. Raw Layer (BigLake - External Data Access):**

* **Sources:** Google Cloud Storage & Cloud Spanner
* **Tool:** BigQuery External Tables (via **BigLake**)
* **Purpose:** Seamless access to external datasets without ingestion

#### **2. ETL Curation Layer:**

* Handled by **BigQuery ETL** using:

  * Data discovery, customization, and munging
  * **Dynamic SQL**, **Stored Procedures**
  * **Merge operations (SCD1, SCD2)**
  * **Audit & Exception Handling**

#### **3. Curated Layer (Internal BigQuery Tables):**

* Tables like `curatedds.cust_part_curated` and `curatedds.cust_part_curated_merge` store transformed data
* Used for analysis and reporting

#### **4. Data Consumption & Visualization:**

* **Google Data Studio**: Dashboards and visual analytics
* **Data Scientists**: Use curated data for **AIML**
* **Business Teams**: Access reports for **decision support systems**

#### **5. Roles Involved:**

* **Data Stewards**: Classify and manage raw data
* **Data Analysts**: Perform in-depth data analysis
* **Clients/Business Users**: Use output for business insights

**Prerequisties:**
```bash
gcloud auth login
```
**Ensure to copy the code into codebase bucket and custs data**
```bash
#Use your local PC/VM and make sure gcloud is already installed
cd ~/Downloads/ 
git clone https://github.com/muralitheda/gcp-cloud-usecases.git #copy his repo url from github  

gsutil cp /home/hduser/Downloads/gcp-cloud-usecases/usecase10-modernization5-gcp-biqquery-serverless-advanced/usecase10_a_consumer_bq_raw_partition_load.sql gs://iz-cloud-training-project-bucket/codebase/
gsutil cp /home/hduser/Downloads/gcp-cloud-usecases/usecase10-modernization5-gcp-biqquery-serverless-advanced/usecase10_b_sp_automation_consumer_bq_raw_partition_load.sql gs://iz-cloud-training-project-bucket/codebase/

```

**Step1 :: Loading the raw partition data into BigQuery**
---

Either in the BQ Console or using bq command run in Cloud shell use
```sql
cd ~/Downloads
gsutil cp gs://iz-cloud-training-project-bucket/codebase/usecase10_a_consumer_bq_raw_partition_load.sql ~/Downloads/

-- Example setup for metadata-driven approach (uncomment and run if 'curatedds.etl_meta' table doesn't exist)
bq query --use_legacy_sql=false 'create table curatedds.etl_meta (id int64,rulesql string);'
bq query --use_legacy_sql=false 'insert into curatedds.etl_meta values(3,"gs://iz-cloud-training-project-bucket/data/custs_header_20250701");'

--=========================== 1st LOAD ================================
-- Checking the data
gsutil cat gs://iz-cloud-training-project-bucket/data/custs_header_20250701

custid,firstname,lastname,age,profession,upd_ts
4000011,Francis,McNamara,47,Therapist,2024-08-01 00:00:01
4000012,Sandy,Raynor,26,Writer,2024-08-01 00:00:01
4000013,Marion,Moon,41,Carpenter,2024-08-01 00:00:01

-- Main Execution
bq query --use_legacy_sql=false < usecase10_a_consumer_bq_raw_partition_load.sql

-- Verification
bq query --use_legacy_sql=false 'select * from rawds.cust_ext order by upd_ts;'
+---------+-----------+----------+-----+------------+---------------------+
| custno  | firstname | lastname | age | profession |       upd_ts        |
+---------+-----------+----------+-----+------------+---------------------+
| 4000011 | Francis   | McNamara |  47 | Therapist  | 2024-08-01 00:00:01 |
| 4000012 | Sandy     | Raynor   |  26 | Writer     | 2024-08-01 00:00:01 |
| 4000013 | Marion    | Moon     |  41 | Carpenter  | 2024-08-01 00:00:01 |
+---------+-----------+----------+-----+------------+---------------------+

bq query --use_legacy_sql=false 'select * from curatedds.cust_part_curated_scd2_append order by upd_ts;'
+---------+------------------+-----+------------+------------+---------------------+
| custno  |       name       | age | profession |   datadt   |       upd_ts        |
+---------+------------------+-----+------------+------------+---------------------+
| 4000011 | Francis,McNamara |  47 | Therapist  | 2025-07-01 | 2024-08-01 00:00:01 |
| 4000012 | Sandy,Raynor     |  26 | Writer     | 2025-07-01 | 2024-08-01 00:00:01 |
| 4000013 | Marion,Moon      |  41 | Carpenter  | 2025-07-01 | 2024-08-01 00:00:01 |
+---------+------------------+-----+------------+------------+---------------------+

bq query --use_legacy_sql=false 'select * from curatedds.cust_part_curated_scd1_merge order by upd_ts;'
+---------+------------------+-----+------------+------------+---------------------+
| custno  |       name       | age | profession |   datadt   |       upd_ts        |
+---------+------------------+-----+------------+------------+---------------------+
| 4000011 | Francis,McNamara |  47 | Therapist  | 2025-07-01 | 2024-08-01 00:00:01 |
| 4000012 | Sandy,Raynor     |  26 | Writer     | 2025-07-01 | 2024-08-01 00:00:01 |
| 4000013 | Marion,Moon      |  41 | Carpenter  | 2025-07-01 | 2024-08-01 00:00:01 |
+---------+------------------+-----+------------+------------+---------------------+

--=========================== 2nd LOAD ================================
-- Checking the data
gsutil cat gs://iz-cloud-training-project-bucket/data/custs_header_20250702

custid,firstname,lastname,age,profession
4000012,Sandy,Raynor,26,Editor,2025-07-02 00:10:01
4000019,Kristine,Dougherty,63,Financial analyst,2025-07-02 00:00:01
4000020,Crystal,Powers,67,Engineering technician,2025-07-02 00:00:01

--Prepare data
bq query --use_legacy_sql=false 'update curatedds.etl_meta set rulesql="gs://iz-cloud-training-project-bucket/data/custs_header_20250702" where id =3'

-- Main Execution
bq query --use_legacy_sql=false < usecase10_a_consumer_bq_raw_partition_load.sql

-- Verification
bq query --use_legacy_sql=false 'select * from rawds.cust_ext order by upd_ts;'
+---------+-----------+-----------+-----+------------------------+---------------------+
| custno  | firstname | lastname  | age |       profession       |       upd_ts        |
+---------+-----------+-----------+-----+------------------------+---------------------+
| 4000019 | Kristine  | Dougherty |  63 | Financial analyst      | 2025-07-02 00:00:01 |
| 4000020 | Crystal   | Powers    |  67 | Engineering technician | 2025-07-02 00:00:01 |
| 4000012 | Sandy     | Raynor    |  26 | Editor                 | 2025-07-02 00:10:01 |
+---------+-----------+-----------+-----+------------------------+---------------------+

bq query --use_legacy_sql=false 'select * from curatedds.cust_part_curated_scd2_append order by upd_ts;'
+---------+--------------------+-----+------------------------+------------+---------------------+
| custno  |        name        | age |       profession       |   datadt   |       upd_ts        |
+---------+--------------------+-----+------------------------+------------+---------------------+
| 4000011 | Francis,McNamara   |  47 | Therapist              | 2025-07-01 | 2024-08-01 00:00:01 |
| 4000012 | Sandy,Raynor       |  26 | Writer                 | 2025-07-01 | 2024-08-01 00:00:01 |
| 4000013 | Marion,Moon        |  41 | Carpenter              | 2025-07-01 | 2024-08-01 00:00:01 |
| 4000019 | Kristine,Dougherty |  63 | Financial analyst      | 2025-07-02 | 2025-07-02 00:00:01 |
| 4000020 | Crystal,Powers     |  67 | Engineering technician | 2025-07-02 | 2025-07-02 00:00:01 |
| 4000012 | Sandy,Raynor       |  26 | Editor                 | 2025-07-02 | 2025-07-02 00:10:01 |
+---------+--------------------+-----+------------------------+------------+---------------------+

bq query --use_legacy_sql=false 'select * from curatedds.cust_part_curated_scd1_merge order by upd_ts;'
+---------+--------------------+-----+------------------------+------------+---------------------+
| custno  |        name        | age |       profession       |   datadt   |       upd_ts        |
+---------+--------------------+-----+------------------------+------------+---------------------+
| 4000011 | Francis,McNamara   |  47 | Therapist              | 2025-07-01 | 2024-08-01 00:00:01 |
| 4000013 | Marion,Moon        |  41 | Carpenter              | 2025-07-01 | 2024-08-01 00:00:01 |
| 4000019 | Kristine,Dougherty |  63 | Financial analyst      | 2025-07-02 | 2025-07-02 00:00:01 |
| 4000020 | Crystal,Powers     |  67 | Engineering technician | 2025-07-02 | 2025-07-02 00:00:01 |
| 4000012 | Sandy,Raynor       |  26 | Editor                 | 2025-07-02 | 2025-07-02 00:10:01 |
+---------+--------------------+-----+------------------------+------------+---------------------+

```

**Step2 :: Loading the raw partition data into BigQuery using stored procedure automation**
---

Either in the BQ Console or using bq command run in Cloud shell use
```sql
cd ~/Downloads
gsutil cp gs://iz-cloud-training-project-bucket/codebase/usecase10_b_sp_automation_consumer_bq_raw_partition_load.sql ~/Downloads/
gsutil cp gs://iz-cloud-training-project-bucket/codebase/usecase10-modernization5-gcp-biqquery-serverless-advanced/usecase10_b_sp_automation_consumer_bq_raw_partition_load.sql ~/Downloads/

--=========================== 1st LOAD ================================
-- Checking the data
gsutil cat gs://iz-cloud-training-project-bucket/data/custs_header_20250701

custid,firstname,lastname,age,profession,upd_ts
4000011,Francis,McNamara,47,Therapist,2024-08-01 00:00:01
4000012,Sandy,Raynor,26,Writer,2024-08-01 00:00:01
4000013,Marion,Moon,41,Carpenter,2024-08-01 00:00:01

-- Stored procedure creation in BQ
bq query --use_legacy_sql=false < usecase10_b_sp_automation_consumer_bq_raw_partition_load.sql

-- Execution
bq query --use_legacy_sql=false "CALL curatedds.sp_cust_etl('gs://iz-cloud-training-project-bucket/data/custs_header_20250701','scd1');"

-- Verification
bq query --use_legacy_sql=false 'select * from rawds.cust_ext order by upd_ts;'
bq query --use_legacy_sql=false 'select * from curatedds.cust_part_curated order by upd_ts;'
bq query --use_legacy_sql=false 'select * from curatedds.cust_part_curated_merge order by upd_ts;'

```


### 🎯 **Key Learning Outcomes:**

* Using **BigLake as external tables** in BigQuery
* Designing **serverless pipelines with stored procedures**
* Applying **SCD1/SCD2 merge logic** for historical tracking
* Building dashboards and ML-ready datasets from curated layers

