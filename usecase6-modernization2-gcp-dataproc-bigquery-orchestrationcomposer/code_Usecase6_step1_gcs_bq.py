#prerequisites
#gcloud dataproc jobs submit pyspark --cluster=wd28cluster --region=us-east1 /home/hduser/install/gcp/pyspark_bq.py
#gcloud dataproc jobs submit pyspark --cluster=wd28cluster --region=us-east1 --jars gs://com-inceptez-data/jars/spark-3.1-bigquery-0.27.1-preview.jar,gs://com-inceptez-data/jars/gcs-connector-latest-hadoop2.jar /home/hduser/install/gcp/gcsToBQRawToBQCurated.py
from pyspark.sql.functions import *
from pyspark.sql.types import *
def main():
   from pyspark.sql import SparkSession
   # define spark configuration object
   spark = SparkSession.builder\
      .appName("GCP GCS Read & Write to BigQuery") \
      .getOrCreate()
   spark.sparkContext.setLogLevel("ERROR")
   sc=spark.sparkContext
   conf = spark.sparkContext._jsc.hadoopConfiguration()
   conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
   conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
   print("Usecase5: Spark Application to Read from GCS and load to Bigquery (Raw) and load into another Bigquery (Curated) table in the GCP")
   print("1. Ingestion layer data read from GCS - brought by some data producers")
   gcs_df = spark.read.option("header", "false").option("delimiter", ",").option("inferschema", "true")\
   .csv("gs://inceptez-common-bucket/dataset/custs").toDF("custno","firstname","lastname","age","profession")
   gcs_df.show()
   print("1. GCS Read Completed Successfully")
   print("Ensure to create the BQ datasets (rawds & curatedds) -> table creation (optional) ")
   print("2. Writing GCS data to Raw BQ table")
   # We need to set the below two properties to enable the inline view queries and dataset info
   #spark.conf.set("viewsEnabled","true")
   #spark.conf.set("materializationDataset","rawds")
   #sourcesystem data -> pyspark DF -> write to temporaryGcsBucket -> read from GCS using a viewsEnabled -> store the final result materializationDataset -> 'rawds.customer_raw'
   #db -> sqoop import -> hive (sqoop imports data from db and store the interiem data into HDFS (temp loc) -> hive table (load data inpath)
   # We need to set the below properties to enable the temp GCS location (change to your GCS location) for bq write
   gcs_df.write.mode("overwrite").format('com.google.cloud.spark.bigquery.BigQueryRelationProvider') \
   .option("temporaryGcsBucket",'inceptez-common-bucket/tmp')\
   .option('table', 'rawds.customer_raw') \
   .save()
   #Execute the below steps if we have a seperate spark pipeline running to read data from BQ raw to the BQ curated 
   # gcs -> bqRawwrite -> bqRawread -> bqCuratedwrite
   #(provided if the raw ingestion is managed by ingestion team and curation is taken care by curation team)
   #print("Reading data from raw table and writing to BQ table in case if we create it as a seperate pipeline")
   #sql = """select custno, concat(firstname,",", lastname) as name, age, coalesce(profession,"unknown") as profession from rawds.customer_raw where age>30""" #pushdown optimization
   #print("raw BQ table to Curated BQ table load completed")
   #df = spark.read.format("bigquery").load(sql)
   #df.write.mode("overwrite").format('bigquery').option("temporaryGcsBucket",'incpetez-data-samples/tmp').option('table', 'curatedds.customer_curated').save()
   print("3. GCS to Raw BQ raw table loaded")
   # gcs -> bqRawwrite 
   #     -> bqCuratedwrite
   gcs_df.createOrReplaceTempView("raw_view")
   curated_bq_df=spark.sql("select custno, concat(firstname,',', lastname) as name, age, coalesce(profession,'unknown') as profession from raw_view where age>30")
   print("Read from rawds is completed") 
   # We need to set the below propertie to enable the temp GCS location for bq write
   curated_bq_df.write.mode("overwrite").format('com.google.cloud.spark.bigquery.BigQueryRelationProvider') \
   .option("temporaryGcsBucket",'inceptez-common-bucket/tmp')\
   .option('table', 'curatedds.customer_curated') \
   .save()
   print("GCS to Curated BQ table load completed")
main()