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

## GCP Cloud Architecture

The core of this solution resides within the Google Cloud Platform, leveraging various services to create a robust data processing and analysis environment.

**Core Components:**

* **Centralized Metastore (Google Cloud Storage):** Acts as a centralized metadata repository, likely for Hive tables and other data assets.
* **Dataproc LR Cluster:** A long-running (LR) Dataproc cluster serves as the primary processing engine.
    * **Spark SQL:** Used for data manipulation and querying.
    * **Hive HDFS:** Provides a distributed file system and data warehousing capabilities within the cluster.
    * **Apache Zeppelin:** Likely used for interactive data analytics and collaboration.
    * **YARN:** Manages cluster resources.
    * **Spark Submit SQL:** Indicates the method for submitting Spark jobs.
* **BigQuery:** A fully managed, serverless data warehouse for analytics.
* **Notebook for Analyzing/Developing BigData Ecosystems:** Suggests the use of managed notebooks for interactive data exploration and development.

**Workflow:**

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

* **Health Tracking**
* **Prevent Fraud & Abuse**
* **Predictive Analytics**
* **Customized Care**
* **Preventing Human Errors**
* **Most Effective Diagnostic**
* **Computational Phenotyping**
* **Patient Similarity**
* **Telemedicine**
* **Medical Imaging**

---