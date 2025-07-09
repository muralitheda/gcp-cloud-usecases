# Hybrid Cloud Data Migration - Key Points


## 1. On-Premises Data Flow (Source)

* **Txns Data (in HDFS)**
    * ETL Process
        * [Hive: Raw Data]
            * ETL Process
                * [Hive: Curated Data]
* **[Hive: Curated Data]**
    * Hadoop User Environment (HUE)



## 2. Data Migration: "Lift & Shift"

* **On-Premises HDFS ===> Google Cloud Storage (GCS)**



## 3. GCP Dataproc Processing (Target)

* **Google Cloud Storage (GCS)**
    * ETL Process
        * [Hive: Raw Data]
            * ETL Process
                * [Hive: Curated Data]
* **[Hive: Curated Data]**
    * Hadoop User Environment (HUE)



## 4. Hybrid Cloud Use Case Steps (Example: Use Case 3)

* **Create Dataproc Cluster**
    * **Connect (SSH / gcloud cli)**
        * **Develop HQL Scripts (for Hive)**
            * **Run gcloud hive job**
                * **Schedule Job (via Cron/Oozie on Edge Node)**
                    * **Verify Data in Hive Tables (on Dataproc)**



## 5. Key Benefits / Applications

* Predictive Analytics
* Fraud & Abuse Prevention
* Improved Diagnostics



## 6. Execution Steps

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

sudo su hdfs  
hadoop fs -mkdir -p /user/hduser/project  
hadoop fs -chmod -R 777 /user/hduser/  
exit
```

**Get the data and code ready in the Dataproc cluster environment to use later (2. Online Transfer)**
```bash
hadoop fs -cp -f gs://iz-cloud-training-project-bucket/txns hdfs:///user/hduser/project/ #data will be loaded by source providers in a frequent interval
```
**3. Create the below cust_etl.hql (load from the raw table to the curated external table) and place it in the gcs bucket.**

```bash
vi cust_etl.hql  
```
```bash
set mapreduce.input.fileinputformat.split.maxsize= 1000000;  
set mapreduce.job.reduces=4;  
set hive.exec.dynamic.partition.mode=nonstrict;  

create external table if not exists ext_transactions(txnno INT, txndate STRING, custno INT, amount DOUBLE,category string, product STRING, city STRING, state STRING, spendby STRING) partitioned by (load_dt STRING)  
row format delimited fields terminated by ','  
stored as textfile  
location '/user/hduser/hiveexternaldata';  

Insert into table ext_transactions partition (load_dt)  select txnno,txndate,custno,amount,category, product,city,state,spendby, current_date() from transactions;**
```

```bash
hadoop fs -put cust_etl.hql /user/hduser/project/
```

**4. Create & Run the below script or schedule in cron to run once in a day (In our ONPREM Centos VM):**  
```bash
vi gcp_hive_schedule.sh
```
```bash
#!/bin/bash  
#source /home/hduser/.bashrc  
echo "`date` gcloud hive ETL script is started"  
echo "`date` gcloud hive ETL script is started" &>> /tmp/gcp_hive_schedule.log  
#hive -e "create table if not exists transactions (txnno INT, txndate STRING, custno INT, amount DOUBLE,category string, product STRING, city STRING, state STRING, spendby STRING) row format delimited fields terminated by ',' stored as textfile"  

gcloud dataproc jobs submit hive --cluster=cluster-dataproc-2 --region us-central1 -e "create table if not exists transactions (txnno INT, txndate STRING, custno INT, amount DOUBLE,category string, product STRING, city STRING, state STRING, spendby STRING) row format delimited fields terminated by ',' stored as textfile" &> /tmp/gcp_hive_schedule.log**  

if [ $? -ne 0 ]  
then  
echo "`date` error occured in the hive table creation part of the EL" &>> /tmp/gcp_hive_schedule.log  
else  
echo "`date` hive table creation part of the EL is completed successfully" &>> /tmp/gcp_hive_schedule.log  
fi   
#in onprem I was calling hive queries like this “hive –e "load data inpath '/user/hduser/project/txns' overwrite into table transactions" “  

gcloud dataproc jobs submit hive --cluster=cluster-dataproc-2 --region us-central1 -e "load data inpath '/user/hduser/project/txns' overwrite into table transactions" &> /tmp/gcp_hive_schedule.log**  

if [ $? -ne 0 ]  
then  
echo "`date` error occured in the hive table load part of the EL" &>> /tmp/gcp_hive_schedule.log  
else  
echo "`date` hive table load part of the EL is completed successfully" &>> /tmp/gcp_hive_schedule.log  
fi   
loaddt=$(date '+%Y-%m-%d')  
#hive –f hdfs:///user/hduser/project/cust_etl.hql in our onprem  

gcloud dataproc jobs submit hive --cluster=cluster-dataproc-2 --region us-central1 --file=hdfs:///user/hduser/project/cust_etl.hql --continue-on-failure \
--params=load_dt=$loaddt &>> /tmp/gcp_hive_schedule.log**    

if [ $? -ne 0 ]  
then  
echo "`date` error occured in the hive table creation part of the EL" &>> /tmp/gcp_hive_schedule.log  
else  
echo "`date` hive table creation part of the EL is completed successfully" &>> /tmp/gcp_hive_schedule.log  
fi  
echo "`date` gcloud hive ETL script is completed" &>> /tmp/gcp_hive_schedule.log  
```

**5. To run the script manually in ONPrem (Testing)** 
```bash
bash gcp_hive_schedule.sh
```

**Data validation in Hive Instance**  
```sql
$ hive  
hive> show tables;  
hive> select count(1) from transactions;  
hive> select count(1) from ext_transactions;  
```

**6. Schedule the above script in the Onprem edge node using the cron tab or (you can ask your Organization scheduling & Monitoring team)**  
```bash
crontab -e  
*/5 * * * * bash /home/hduser/gcp_hive_schedule.sh
```

**To know the scheduled jobs list**
```bash
crontab -l
```
