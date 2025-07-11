# Spark Application: HDFS to GCS Data Transfer (Online Transfer)

This guide provides instructions for setting up a Spark application to transfer data from an on-premise **Hadoop Distributed File System (HDFS)** to a **Google Cloud Storage (GCS)** bucket. This is crucial for **Cloud Data Engineers** working on hybrid data migration scenarios.

-----

## Prerequisites

Before running the Spark application, ensure you have the following:

1.  Access to a Google Cloud Platform (GCP) project.
2.  A VM with HDFS access (e.g., a Hadoop Datalake environment).
3.  Necessary data prepared in HDFS.
4.  The required GCS connector JAR file.

-----

## Setup Instructions

### 1\. Google Cloud Service Account Configuration

This section focuses on setting up the necessary credentials for accessing GCS.

1.  **Create a Service Account** in the Google Cloud Console.
2.  **Assign Roles**: Grant the service account **`Storage Admin`** or **`Storage Editor`** roles to enable data transfer to GCS.
3.  **Download Credentials**: Download the service account's JSON credential file and place it in the VM at `/home/hduser/gcp/`.

> **Note:** If you were provided with a service account JSON file, you can use that instead of creating a new one.

### 2\. GCS Connector Setup

The **GCS connector** allows Spark to communicate with Google Cloud Storage.

1.  **Download the Connector JAR**: Copy the `gcs-connector-hadoop2-2.2.7.jar` file from the specified Google Cloud Storage location to your VM.

    ```bash
    gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop2-2.2.7.jar /home/hduser/gcp/
    ```

2.  **Update Spark Configuration**: Ensure the connector JAR path is included in your Spark application configuration (`mypyspark.jars`).

### 3\. Data Preparation (HDFS Datalake)

Prepare the data you intend to transfer within your HDFS environment.

1.  **Create HDFS Directory**:

    ```bash
    hadoop fs -mkdir datatotransfer
    ```

2.  **Upload Data**: Copy local data to the HDFS directory.

    ```bash
    hadoop fs -put /home/hduser/hive/data/custs datatotransfer/
    ```

### 4\. Hive Table Setup (Optional)

If your application requires data from Hive, ensure the necessary table is available.

1.  **Create Database**:

    ```sql
    CREATE DATABASE IF NOT EXISTS retail;
    ```

2.  **Create Table `txnrecords`**:

    ```sql
    CREATE TABLE txnrecords(
        txnno INT,
        txndate STRING,
        custno INT,
        amount DOUBLE,
        category STRING,
        product STRING,
        city STRING,
        state STRING,
        spendby STRING
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;
    ```

3.  **Load Data**:

    ```sql
    LOAD DATA LOCAL INPATH '/home/hduser/hive/data/txns' INTO TABLE txnrecords;
    ```

### 5\. Verify Files in VM

Ensure the necessary files (service account JSON, GCS connector JAR, and Spark job script) are located in `/home/hduser/gcp/`.

```bash
ls -lrt /home/hduser/gcp
/home/hduser/gcp/some-sa-key.json
/home/hduser/gcp/gcs-connector-hadoop2-2.2.7.jar
/home/hduser/gcp/usecase_1_2_job.py
```

### 6\. Submitting the Spark Job

Submit the PySpark job, ensuring you include the GCS connector JAR in the `--jars` parameter.

> **Important:** If you have previously copied the `gcs-connector-hadoop-latest.jar` to `/usr/local/mypyspark/jars`, remove it to avoid conflicts.

```bash
spark-submit --jars /home/hduser/gcp/gcs-connector-hadoop2-2.2.7.jar /home/hduser/gcp/usecase_1_2_job.py
```

-----

## Next Steps

After running the job, verify the data transfer by checking the target GCS bucket. If you encounter errors, review the configurations, particularly the service account permissions and the path to the GCS connector JAR.