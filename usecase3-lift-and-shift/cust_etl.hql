set mapreduce.input.fileinputformat.split.maxsize= 1000000;
set mapreduce.job.reduces=4;
set hive.exec.dynamic.partition.mode=nonstrict;

create external table if not exists ext_transactions(txnno INT, txndate STRING, custno INT, amount DOUBLE,category string, product STRING, city STRING, state STRING, spendby STRING) partitioned by (load_dt STRING)
row format delimited fields terminated by ','
stored as textfile
location '/user/hduser/hiveexternaldata';

Insert into table ext_transactions partition (load_dt)  select txnno,txndate,custno,amount,category, product,city,state,spendby, current_date() from transactions;