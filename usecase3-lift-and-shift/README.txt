# Hybrid Cloud Data Migration - Key Points

## 1. On-Premises Data Flow (Source)

   Txns Data (in HDFS)
       --> ETL Process
             --> [Hive: Raw Data]
                   --> ETL Process
                         --> [Hive: Curated Data]

   [Hive: Curated Data]
       --> Hadoop User Environment (HUE)
   =================================================================


## 2. Data Migration: "Lift & Shift"

   On-Premises HDFS
       ===> (hadoop fs -cp gs:// ) ===> Google Cloud Storage (GCS)
   =================================================================


## 3. GCP Dataproc Processing (Target)

   Google Cloud Storage (GCS)
       --> ETL Process
             --> [Hive: Raw Data]
                   --> ETL Process
                         --> [Hive: Curated Data]

   [Hive: Curated Data]
       --> Hadoop User Environment (HUE)
   =================================================================


## 4. On-Premises Edge Node: Orchestration

   [On-Premises Edge Node]
       --> Cron/Scheduler
             --> Shell Script: gcp_hive_schedule.sh
                   --> Triggers GCLOUD Cron Jobs
                         --> [GCP Dataproc Cluster] (Executes Hive/Spark Jobs)
   =================================================================


## 5. Hybrid Cloud Use Case Steps (Example: Use Case 3)

   1. Create Dataproc Cluster
      --> 2. Connect (SSH / gcloud cli)
          --> 3. Develop HQL Scripts (for Hive)
              --> 4. Run gcloud hive job
                  --> 5. Schedule Job (via Cron/Oozie on Edge Node)
                      --> 6. Verify Data in Hive Tables (on Dataproc)
   =================================================================


## 6. Key Benefits / Applications

   - Health Tracking
   - Predictive Analytics
   - Customized Care
   - Medical Imaging
   - Fraud & Abuse Prevention
   - Improved Diagnostics
   =================================================================