declare loadts timestamp;
set loadts=(select current_timestamp());
--loadts=2023-09-10 10:23:10
begin
begin
--How to achieve Complete Refresh (delete the entire target data and reload with the entire source data)
--option1:drop the existing table and recreate the table with the same structure then later reload the entire customer data (both structure and data deleted)
--option2:Truncate table and insert option can be used to load the table later (if the entire data has to be deleted) (entire data (only) deleted)
--option2:****insert overtwrite into table option is not supported in BQ as like hive
--option3:delete table (only the given date/hour) and insert option can be used later (entire/portion of data (only) deleted)
--interview questions: Difference between drop (create or replace table) (preference2), truncate (preference1) and delete(preference3).
--interview questions: Difference between Hive & Bigquery - (Bigquery doesn't support insert overtwrite and hive doesn't support  create or replace table.
--interview questions: Difference between Hive & Bigquery - (Bigquery support delete and hive doesn't support Delete directly (we need some workaround)
create schema if not exists curatedds options(location='us-central1');

create or replace table curatedds.consumer_full_load(
custno INT64, 
fullname STRING,
age INT64,
yearofbirth INT64,
profession STRING,
loadts timestamp,
loaddt date);

--truncate table curatedds.consumer_full_load;
--delete from curatedds.consumer_full_load where filters....;

--Biquery will support only date or integer columns type and not string for partitioning, where as hive supports string also
--Biquery will support only single partitioning columns (no multi level partition), where as hive supports multiple (multi level) partitions columns
--Syntax wise, bigquery can have the column for partition in both create table columns list and partition list also, where as hiveonly support the column in the partition which should not be defined in the create table columns list.
--Partition management in Bigquery is simplified eg. We don't have msck repair/refreshing of the partition by keeping data in hdfs are not needed as like hive (because hive has metadata layer and data layer seperated).

--*** Important - challenge in migrating from hive to BQ
-- Since, insert overtwrite into table option is not supported in BQ as like hive, we can't do insert overwrite partition also.
-- as a workaround, we have to delete the data seperately, then load into the partition table. delete from table where loaddt=loaddt(variable)

--Partitions can be created only on the low cardinal Date & Number columns, Maximum number of partitions can be only 4000

create table if not exists curatedds.trans_online_part
(transsk numeric,customerid int64,productname string,productcategory string,productprice int64,productidint int64,prodidstr string ,loadts timestamp,loaddt date)
partition by loaddt
OPTIONS (require_partition_filter = FALSE);
--require_partition_filter will enforce a mandatory partition filter to be used mandatorily for the user of the table

--Bigquery supports clustering (sorting and grouping/bucketing of the clustered columns) to improve filter and join performances
--BQ clustering is exactly equivalent to hive bucketing only syntax varies, symantics remains same...
--bq syntax(cluster by col1,col2..), hive syntax is (clustered by col1,col2... into 3 buckets sort by col1,col2)

create table  if not exists curatedds.trans_pos_part_cluster
(txnno numeric,txndt date,custno int64,amount float64,category string,product string,city string, state string, spendby string,loadts timestamp,loaddt date) 
partition by loaddt
cluster by loaddt,custno,txnno
OPTIONS (description = 'point of sale table with partition and clusters');
end;

--Block1 for loading raw consumer data to the curated consumer table (Full Refresh) - using inline views (some name for the memory)
begin
--truncate table curatedds.consumer_full_load;
--clensing - de duplication
--scrubbing - replacing of null with some values
--curation (business logic) - concat 
--Data enrichment - deriving a column from exiting or new column
--inline view (from clause subquery) is good for nominal data size, but not good for holding large volume of data in memory.. if volume is large, go with temp view.
insert into curatedds.consumer_full_load
SELECT custid,concat(firstname,' ',lastname),age,
extract(year from date_add(current_date,interval -`age`*12 month)) as yearofbirth,
--extract(year from current_date)-`age` as yearofbirth,
--case when profession is null then 'not provided' else profession end as profession,
coalesce(profession,'not provided') profession
,loadts,date(loadts) as loaddt 
FROM (select custid,firstname,lastname,age,profession from 
		(select custid,firstname,lastname,age,profession,row_number() over(partition by custid order by age desc) rnk 
			from rawds.consumer)inline_view
	  where rnk=1)dedup;
end;

begin
--Temporary table concept in BQ - Temp table is a session table, that holds data in the underlying colossus store only rather than memory as like inline view, but exist only till the session exist (begin end).
--When we go for temp table, if we get some performance issue while handling very large volume of data (in inline views) which can't be hold in the memory, then use temp view.
--Temp table will help you reuse of some query result in multiple places inside the given block.

--Curation Logic - 
--Converting from Semistructured to structured using unnest function (equivalent to explode in hive)
--generating surrogate key (unique identifier)
--Splitting of columns by extracting the string and number portion from the product column 
--select regexp_replace('4B2A', '[^0-9]','')
--below stardardization will convert blankspace/nulls/number only to blankspace then to 'na', allow only the string portition to the target otherwise
--case when coalesce(trim(regexp_replace(lower(prod_exploded.productid),'[^a-z]','')),'')='' then 'na' 
--adding some date and timestamp columns for load data.
--Reusability feature of temp table and performance feature of temp table.
--generate_uuid will generate unique id alphanumeric value, convert to number (hashing) using farm_fingerprint, make it positive value using abs function

create or replace temp table online_trans_view as 
select abs(farm_fingerprint(generate_uuid())) as transk
,customerid
,prod_exploded.productname
,prod_exploded.productcategory
,prod_exploded.productprice
,cast(regexp_replace(prod_exploded.productid,'[^0-9]','') as int64) as prodidint
,case when coalesce(trim(regexp_replace(lower(prod_exploded.productid),'[^a-z]','')),'')='' 
then 'na' 
else regexp_replace(lower(prod_exploded.productid),'[^a-z]','')
end as prodidstr,
loadts,
date(loadts) as loaddt
from `rawds.trans_online` p,unnest (products) as prod_exploded;

----Nested
--4000015,[{Table,Furniture,440,34P},{Chair,Furniture,440,42A}]
--Unnested
--4000015,Table,Furniture,440,34P
--4000015,Chair,Furniture,440,42A

--visagan - I saw this delete statement based on load date with in our project in the begining of the each query file  and they said its for job restartability/rerun of the jobs
delete from curatedds.trans_online_part where loaddt=date(loadts);

--as like hive, we don't need parition(), we can't use insert overwrite - 
--insert overwrite table curatedds.trans_online_part partition(loaddt) select * from online_trans_view;
--in BQ we have to delete the partition explicitly and load the data, if any data is going to be repeated.
--Loading of partition table in BQ, 

insert into curatedds.trans_online_part select * from online_trans_view;

--to understand reusability feature of the temp table.
--how to create a table structure from another table without loading data.
create table if not exists curatedds.trans_online_part_furniture as select * from online_trans_view where 1=2;
truncate table curatedds.trans_online_part_furniture;
insert into curatedds.trans_online_part_furniture select * from online_trans_view where productcategory='Furniture';

end;

begin
--CURATION logic
--date format conversion and casting of string to date
--data conversion of product to na if null is there
--converted columns/some of the selected columns in an order, are not selected again by using except function
insert into curatedds.trans_pos_part_cluster 
SELECT txnno,parse_date('%m-%d-%Y',txndt) txndt,custno,amt,coalesce(product,'NA'),* except(txnno,txndt,custno,amt,product),loadts,date(loadts) as loaddt 
FROM `rawds.trans_pos`;
end;

begin
--curation logic
--In the below usecase, storing the data into 3 year tables, because ...
--To improve the performance of picking only data from the given year table
--To reduce the cost of storing entired data in more frequent data access layer (active storage), rather old year may be accessed once in a quarter or half yearly or yearly which data is stored in (long term storage) of BQ saves the cost.
--Drawback is - we need to combine all these tables using union all or union distinct to get all table data in one shot, but BQ 
--provides a very good feature of wild card search for the table names... eg. select * from `dataset.tablename*`
--merging of columns of date and time to timestamp column
--merging of long and lat to geopoint data type to plot in the google map to understand where was my customer when the trans was happening

--Usecases: autopartition (without defining our own partition, BQ itself will create and maintain the partition of the internal clock load date) - simply we can still create date partitions if we don't have date data in our dataset.
--geography datatype - is used to hold the long,lat gps (geo) coordinates of the location.
--wildcard table queries
--table segregation for all the above reasons mentioned.
--for current year 2023 date alone, we are making the future date as current date using INLINE VIEW/FROM CLAUSE SUBQUERY, to correct the data issue from the source.

CREATE TABLE if not exists curatedds.trans_mobile_autopart_2021
(txnno numeric,dt date,ts timestamp,geo_coordinate geography,net string,provider string,activity string,postal_code int,town_name string,loadts timestamp,loaddt date)
PARTITION BY
  _PARTITIONDATE;

insert into `curatedds.trans_mobile_autopart_2021` (txnno,dt,ts,geo_coordinate,net,provider,activity,postal_code,town_name,loadts,loaddt)
select txnno,cast(dt as date) dt,timestamp(concat(dt,' ',hour)) as ts,ST_GEOGPOINT(long,lat) geo_coordinate,net,provider,activity,postal_code,town_name,loadts,date(loadts) as loaddt
from `rawds.trans_mobile_channel`
where extract(year from cast(dt as date))=2021;


CREATE TABLE  if not exists curatedds.trans_mobile_autopart_2022
(txnno numeric,dt date,ts timestamp,geo_coordinate geography,net string,provider string,activity string,postal_code int,town_name string,loadts timestamp,loaddt date)
PARTITION BY
  _PARTITIONDATE;

insert into curatedds.trans_mobile_autopart_2022 (txnno,dt,ts,geo_coordinate,net,provider,activity,postal_code,town_name,loadts,loaddt)
select txnno,cast(dt as date) dt,timestamp(concat(dt,' ',hour)) as ts,ST_GEOGPOINT(long,lat) geo_coordinate,net,provider,activity,postal_code,town_name,loadts,date(loadts) as loaddt
from `rawds.trans_mobile_channel`
where extract(year from cast(dt as date))=2022;

CREATE TABLE  if not exists curatedds.trans_mobile_autopart_2023
(txnno numeric,dt date,ts timestamp,geo_coordinate geography,net string,provider string,activity string,postal_code int,town_name string,loadts timestamp,loaddt date)
PARTITION BY
  _PARTITIONDATE;

insert into curatedds.trans_mobile_autopart_2023 (txnno,dt,ts,geo_coordinate,net,provider,activity,postal_code,town_name,loadts,loaddt)  
select txnno,cast(dt as date) dt,timestamp(concat(dt,' ',hour)) as ts,ST_GEOGPOINT(long,lat) geo_coordinate,net,provider,activity,postal_code,town_name,loadts,date(loadts) as loaddt
from (select case when cast(dt as date)>current_date() then current_date() else dt end as dt,* except(dt) from `rawds.trans_mobile_channel`) t
where extract(year from cast(dt as date))=2023;
end;

end;