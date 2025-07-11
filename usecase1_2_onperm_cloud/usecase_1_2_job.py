
from pyspark.sql.functions import *
from pyspark.sql.types import *
def main():
   from pyspark.sql import SparkSession
   # define mypyspark configuration object
   spark = SparkSession.builder\
      .appName("HDFS <-> GCS & Hive <-> GCS Read/Write Usecase 1 & 2") \
      .config("mypyspark.jars", "/home/hduser/gcp/gcs-connector-hadoop2-2.2.7.jar")\
      .enableHiveSupport()\
      .getOrCreate()
   #GCS Jar location
   spark.sparkContext.setLogLevel("ERROR")
   conf = spark.sparkContext._jsc.hadoopConfiguration()
   conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
   conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
   conf.set("google.cloud.auth.service.account.json.keyfile","/home/hduser/gcp/gcp-serviceaccount.json")

   print("========================================================================")
   print("===== Usecase 1 - Data Transfer between HDFS to GCS and Vice versa =====")
   print("========================================================================")
   hdfs_df=spark.read.csv("hdfs://localhost:54310/user/hduser/datatotransfer/")
   print("[STEP 1/8] HDFS Read Completed Successfully")
   hdfscnt=hdfs_df.count()
   curts = spark.createDataFrame([1], IntegerType()).withColumn("curts", current_timestamp()).select(date_format(col("curts"), "yyyyMMddHHmmSS")).first()[0]
   print(curts)
   hdfs_df.coalesce(1).write.csv("gs://iz-hdfs-data/custdata_"+curts)
   print("[STEP 2/8] GCS Write Completed Successfully")

   gcs_df = spark.read.option("header", "false").option("delimiter", ",")\
      .csv("gs://iz-hdfs-data/custdata_"+curts).toDF("custid","fname","lname","age","profession")
   gcs_df.cache()
   gcs_df.show(2)
   gcscnt=gcs_df.count()
   #Reconcilation
   if (hdfscnt==gcscnt):
      print("[STEP 3/8] GCS Write Completed Successfully including Data Quality/Reconcilation check completed (equivalent to sqoop --validate)")
   else:
      print("[STEP 3/8] Count is not matching - Possibly GCS Write Issue")
      exit(1)

   print("[STEP 4/8] Reading & Writing data into hive table")
   print("[SPTE 5/8] Writing data from GCS to Hive table")
   gcs_df.write.mode("overwrite").saveAsTable("retail.custs")
   print("[STEP 6/8] GCS to Hive Write Completed Successfully")

   print("[STEP 7/8] Reading data from hive table")
   df_hive=spark.read.table("retail.txnrecords")
   df_hive.write.json("gs://iz-hdfs-data/txndata_json_"+curts)
   print("[STEP 8/8] Hive to GCS Write Completed Successfully")

main()
