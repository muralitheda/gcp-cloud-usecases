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

gcloud auth login

**1. Admin - Once for all - Create a long running dataproc cluster**

gcloud dataproc clusters create cluster-dataproc-2 --enable-component-gateway --bucket iz-dataproc-uscentral1-bucket-1 --region us-central1 --zone us-central1-a --master-machine-type e2-standard-2 --master-boot-disk-size 100 --num-workers 3 --worker-machine-type e2-standard-2 --worker-boot-disk-size 100 --image-version 2.1-rocky8 --properties hdfs:dfs.blocksize=268435456 --max-idle 7200s --project iz-cloud-training-project 

gcloud dataproc clusters describe cluster-dataproc-2 --region=us-central1


**2. Admin - Once for all - Open the Dataproc Master node ssh (edge node of the cloud cluster) and execute the below steps:**

gcloud compute ssh --zone "us-central1-a" "cluster-dataproc-2-m" --project "iz-cloud-training-project"  


**3. Develop the pyspark code & pushed the code (Usecase4_GcpGcsReadWritehive_cloud.py) to GIT**  

https://github.com/muralitheda/gcp-cloud-usecases/blob/master/usecase4-lift-and-shift-git/Usecase4_GcpGcsReadWritehive_cloud.py  

**4.  Login to the dataproc master node using public dns or master node ssh or using vm edge node terminal and install the Git (Admin will do once for all)**  

#sudo yum install git  
git config --global user.name "muralitheda"  
git config --global user.email "murali.balasubramaniam@outlook.com"  
git config --list  
git init  
cd .git/  

**5. Login to master node & Clone the git repo to download the code (Production Deployment Team/CICD Tool)**  

git clone https://github.com/muralitheda/gcp-cloud-usecases.git #copy his repo url from github  











