-- Declare and set is equivalent to assigning a variable with value in other prog. languages
-- eg. in python we create load_ts=datetime.now();
DECLARE loadts TIMESTAMP;
SET loadts = (SELECT CURRENT_TIMESTAMP());
-- loadts = 2025-07-16 08:55:10

BEGIN

    -- How to achieve Complete Refresh (delete the entire target data and reload with the entire source data)
    -- option1: drop the existing table and recreate the table with the same structure then later reload the entire customer data (both structure and data deleted)
    -- option2: Truncate table and insert option can be used to load the table later (if the entire data has to be deleted) (entire data (only) deleted)
    -- option2: ****insert overwrite into table option is not supported in BQ as like hive
    -- option3: delete table (only the given date/hour) and insert option can be used later (entire/portion of data (only) deleted)
    -- interview questions: Difference between drop (create or replace table) (preference2), truncate (preference1) and delete(preference3).
    -- interview questions: Difference between Hive & Bigquery - (Bigquery doesn't support insert overwrite and hive doesn't support create or replace table.
    -- interview questions: Difference between Hive & Bigquery - (Bigquery support delete and hive doesn't support Delete directly (we need some workaround)

    CREATE SCHEMA IF NOT EXISTS curatedds OPTIONS(location='us');

    CREATE OR REPLACE TABLE curatedds.consumer_full_load (
        custno INT64,
        fullname STRING,
        age INT64,
        yearofbirth INT64,
        profession STRING,
        loadts TIMESTAMP,
        loaddt DATE
    );
    -- Interview1: What is the difference between Drop & Create or Truncate or Delete ?
    -- above query will drop the table if exists and recreate it...
    -- TRUNCATE TABLE curatedds.consumer_full_load;
    -- DELETE FROM curatedds.consumer_full_load WHERE filters....;

    -- Interview2:
    -- Tell me the migration/modernization challenges you faced between hive and bigquery?
    -- Tell me about partition concept in Bigquery, why we needed and when do we go for partitioning?
    -- It is same like hive, ie. partition will store the data internally into the given partition that is to improve filter performance by avoiding FTS (FULL TABLE SCAN)
    -- Tell me in a table how many partition columns can be created and what could be the datatypes?
    -- Only ONE (Level) partition column and datatype can be either date/integer and not string
    -- How many values can be kept in a partition column?
    -- maximum 4000 unique values for eg. in a date partition we can store upto 11 years date values (11*365)
    -- & How it is different from Hive?
    -- Bigquery will support only date or integer columns type and not string for partitioning, where as hive supports string and other datatypes also
    -- Bigquery will support only single partitioning columns (no multi level partition), where as hive supports multiple (multi level) partitions columns
    -- Syntax wise, bigquery can have the column for partition in both create table columns list and partition list also, where as hive only support the column in the partition which should not be defined in the create table columns list.
    -- hive -> CREATE TABLE tbl1(id INT) PARTITIONED BY (load_dt DATE);
    -- bq -> CREATE TABLE tbl1(id INTEGER,load_dt DATE) PARTITION BY load_dt;

    -- Partition management in Bigquery is simplified eg. We don't have msck repair/refreshing of the partition by keeping data in hdfs are not needed as like hive (because hive has metadata layer and data layer seperated).

    -- *** Important - challenge in migrating from hive to BQ
    -- Since, insert overwrite into table option is not supported in BQ as like hive, we can't do insert overwrite partition also.
    -- as a workaround, we have to delete the data seperately, then load into the partition table. DELETE FROM table WHERE loaddt = loaddt(variable)

    -- Partitions can be created only on the low cardinal Date & Number columns, Maximum number of partitions can be only 4000

    CREATE TABLE IF NOT EXISTS curatedds.trans_online_part (
        transsk NUMERIC,
        customerid INT64,
        productname STRING,
        productcategory STRING,
        productprice INT64,
        productidint INT64,
        prodidstr STRING,
        loadts TIMESTAMP,
        loaddt DATE
    )
    PARTITION BY loaddt
    OPTIONS (require_partition_filter = FALSE);
    -- require_partition_filter will enforce a mandatory partition filter to be used mandatorily for the user of the table

    -- Interview3: When do we go for Clustering in BQ? For join and filter performance improvement
    -- Maximum cluster by columns can be only 4 in BQ, hive can support any numbers
    -- Difference between Hive bucketing and BQ clustering? Both are same technically, but syntax varies..
    -- Clustering columns order of defining is very important
    -- Bigquery supports clustering (sorting and grouping/bucketing of the clustered columns) to improve filter and join performances
    -- BQ clustering is exactly equivalent to hive bucketing only syntax varies, symantics remains same...
    -- bq syntax(cluster by col1,col2 upto max col4), hive syntax is (clustered by col1,col2... INTO 3 BUCKETS SORT BY col1,col2)

    CREATE TABLE IF NOT EXISTS curatedds.trans_pos_part_cluster (
        txnno NUMERIC,
        txndt DATE,
        custno INT64,
        amount FLOAT64,
        category STRING,
        product STRING,
        city STRING,
        state STRING,
        spendby STRING,
        loadts TIMESTAMP,
        loaddt DATE
    )
    PARTITION BY loaddt
    CLUSTER BY loaddt, custno, txnno
    OPTIONS (description = 'point of sale table with partition and clusters');

END;

-- Block1 for loading raw consumer data to the curated consumer table (Full Refresh) - using inline views (some name for the memory)
BEGIN

    -- Interview4: Difference between delete, truncate and drop+recreate (create or replace table)
    -- delete -> DML, it help us delete the portion of data applying where clause and in BQ it is mandatory to use where clause (DELETE must have a WHERE clause at [1:1])
    -- truncate -> DDL, it help us delete the data entire data (without affecting the structure) without applying any filters (faster than delete).
    -- drop -> DDL, it help us drop the table structure and data also.
    -- TRUNCATE TABLE curatedds.consumer_full_load;

    -- DATA MUNGING
    -- cleansing - de duplication
    -- scrubbing - replacing of null with some values
    -- curation (business logic) - concat
    -- Data enrichment - deriving a column from exiting or new column
    -- inline view (from clause subquery) is good for nominal data size, but not good for holding large volume of data in memory.. if volume is large, go with temp view.
    ---- FROM CLAUSE SUB QUERY OR INLINE VIEW eg. SELECT cols FROM (SELECT cols FROM tbl) AS view;
    -- We have an alternative for inline view by using qualify function
    -- This is a Slowly Changing Dimension Type 1 Load (Since we are not maintaining History)
    -- TRUNCATE TABLE curatedds.consumer_full_load;
    /*
    #subquery
    SELECT * FROM rawds.consumer WHERE custid = (SELECT MAX(custid) FROM rawds.consumer)
    #correlated subquery (outer query depend on inner query & vice versa)
    For every row of execution of a parent query the child query will be executed and based on the child query
    result the parent query will run..
    SELECT * FROM rawds.consumer parent WHERE age = (SELECT MAX(age) FROM rawds.consumer child WHERE child.custid = parent.custid) AND custid = 4000011
    */
    INSERT INTO curatedds.consumer_full_load
    SELECT
        custid,
        CONCAT(firstname, ' ', lastname) AS fullname,
        age,
        EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE, INTERVAL age YEAR)) AS yearofbirth,
        COALESCE(profession, 'not provided') AS profession,
        loadts,
        DATE(loadts) AS loaddt
    FROM (
        SELECT
            custid,
            firstname,
            lastname,
            age,
            profession
        FROM (
            SELECT
                custid,
                firstname,
                lastname,
                age,
                profession,
                ROW_NUMBER() OVER (PARTITION BY custid ORDER BY age DESC) AS rnk
            FROM rawds.consumer
            --qualify rnk=1 #will help us avoid inline view
        ) AS inline_view
        WHERE rnk = 1
    ) AS dedup;

END;

BEGIN

    -- Interview5:
    -- In BigQuery, a temporary table is a session-scoped table that stores data in the underlying Colossus storage,
    -- not in memory like inline views.
    -- Temporary tables persist only for the duration of the session (such as a BEGIN ... END block or script).
    -- Use temporary tables when working with large intermediate results that may not fit well in memory if handled with inline views.
    -- They improve performance, reusability, and make complex queries easier to structure by storing intermediate results that can be reused multiple times within the same script or session or using the trick of getting the session table rom the job information, we can use it outside of the session also for a day.
    -- Additional Features of temp table for interview purpose.
    /*
    Auto-Cleanup - within the session or actually 1 day expiration
    Faster for Intermediates - since it uses Colossus
    Cost-Efficient - no long term storage (only 1 day max)
    Session-Scoped - no conflict with other sessions
    Helps in cases where Common Table Expressions (CTEs) or Inline view might not perform well
    */

    -- Curation Logic -
    -- Converting from Semistructured to structured using unnest function (equivalent to explode in hive)
    -- generating surrogate key (unique identifier)
    -- Splitting of columns by extracting the string and number portion from the product column
    -- SELECT REGEXP_REPLACE('4B2A', '[^0-9]','')
    -- below stardardization will convert blankspace/nulls/number only to blankspace then to 'na', allow only the string portition to the target otherwise
    -- CASE WHEN COALESCE(TRIM(REGEXP_REPLACE(LOWER(prod_exploded.productid),'[^a-z]','')),'')='' THEN 'na'
    -- adding some date and timestamp columns for load data.

    -- generate_uuid will generate unique id alphanumeric value, convert to number (hashing) using farm_fingerprint, make it positive value using abs function

    /* How the un nesting is happening / semistruct to structure conversion using unnest equivalent to explode in hive
    {"orderId":"1","storeLocation":"Newyork","customerId" : "4000011",
    "products":[{"productId":"1234","productCategory":"Laptop","productName" : "HP Pavilion","productPrice": "540"},
    {"productId":"421","productCategory":"Mobile","productName" : "Samsung S22","productPrice": "240"}]}
    Nested (semi structured):
    orderId,storeLocation,customerId,products
                                     productId,productCategory,productName,productPrice
    1        ,Newyork,       4000011     ,1234,      Laptop,          HP Pavilion,540
                                         421,       Mobile,          Samsung S22,240
    Un Nested (structured):
    orderId,storeLocation,customerId,products.productId,products.productCategory,products.productName,products.productPrice
    1        ,Newyork,       4000011     ,1234,      Laptop,          HP Pavilion,540
    1        ,Newyork,       4000011     ,421,       Mobile,          Samsung S22,240
    */
    CREATE OR REPLACE TEMP TABLE online_trans_view AS
    SELECT
        ABS(FARM_FINGERPRINT(GENERATE_UUID())) AS transk,
        customerid,
        prod_exploded.productname,
        prod_exploded.productcategory,
        prod_exploded.productprice,

        -- Extract numeric part of product ID
        CAST(REGEXP_REPLACE(prod_exploded.productid, '[^0-9]', '') AS INT64) AS prodidint,
        -- Extract alphabetic part or default to 'na'
        CASE
            WHEN COALESCE(TRIM(REGEXP_REPLACE(LOWER(prod_exploded.productid), '[^a-z]', '')), '') = ''
            THEN 'na'
            ELSE REGEXP_REPLACE(LOWER(prod_exploded.productid), '[^a-z]', '')
        END AS prodidstr,
        loadts,
        DATE(loadts) AS loaddt
    FROM `rawds.trans_online` p,
    UNNEST(p.products) AS prod_exploded;

    ----Nested
    --4000015,[{Table,Furniture,440,34P},{Chair,Furniture,440,42A}]
    --Unnested
    --4000015,Table,Furniture,440,34P
    --4000015,Chair,Furniture,440,42A

    -- visagan - I saw this delete statement based on load date with in our project in the begining of the each query file and they said its for job restartability/rerun of the jobs
    DELETE FROM curatedds.trans_online_part WHERE loaddt = DATE(loadts);

    -- as like hive, we don't need parition(), we can't use insert overwrite -
    -- INSERT OVERWRITE TABLE curatedds.trans_online_part PARTITION(loaddt) SELECT * FROM online_trans_view;
    -- in BQ we have to delete the partition explicitly and load the data, if any data is going to be repeated.
    -- Loading of partition table in BQ,

    INSERT INTO curatedds.trans_online_part SELECT * FROM online_trans_view;

    -- to understand reusability feature of the temp table.
    -- how to create a table structure from another table without loading data.
    CREATE TABLE IF NOT EXISTS curatedds.trans_online_part_furniture AS SELECT * FROM online_trans_view WHERE 1 = 2;
    TRUNCATE TABLE curatedds.trans_online_part_furniture;
    INSERT INTO curatedds.trans_online_part_furniture SELECT * FROM online_trans_view WHERE productcategory = 'Furniture';

END;

BEGIN

    -- CURATION logic
    -- date format conversion and casting of string to date
    -- data conversion of product to na if null is there
    -- converted columns/some of the selected columns in an order, are not selected again by using except function
    INSERT INTO curatedds.trans_pos_part_cluster
    SELECT txnno, PARSE_DATE('%m-%d-%Y', txndt) AS txndt, custno, amt, COALESCE(product, 'NA'), * EXCEPT(txnno, txndt, custno, amt, product), loadts, DATE(loadts) AS loaddt
    FROM `rawds.trans_pos`;

END;

BEGIN

    -- curation logic
    -- In the below usecase, storing the data into 3 year tables, because ...
    -- To improve the performance of picking only data from the given year table
    -- To reduce the cost of storing entired data in more frequent data access layer (active storage), rather old year may be accessed once in a quarter or half yearly or yearly which data is stored in (long term storage) of BQ saves the cost.
    -- Drawback is - we need to combine all these tables using union all or union distinct to get all table data in one shot, but BQ
    -- provides a very good feature of wild card search for the table names... eg. select * from `dataset.tablename*`
    -- merging of columns of date and time to timestamp column
    -- merging of long and lat to geopoint data type to plot in the google map to understand where was my customer when the trans was happening

    -- Usecases: autopartition (without defining our own partition, BQ itself will create and maintain the partition of the internal clock load date) - simply we can still create date partitions if we don't have date data in our dataset.
    -- geography datatype - is used to hold the long,lat gps (geo) coordinates of the location.
    -- wildcard table queries
    -- table segregation for all the above reasons mentioned.
    -- for current year 2023 date alone, we are making the future date as current date using INLINE VIEW/FROM CLAUSE SUBQUERY, to correct the data issue from the source.

    CREATE TABLE IF NOT EXISTS curatedds.trans_mobile_autopart_2021 (
        txnno NUMERIC,
        dt DATE,
        ts TIMESTAMP,
        geo_coordinate GEOGRAPHY,
        net STRING,
        provider STRING,
        activity STRING,
        postal_code INT64,
        town_name STRING,
        loadts TIMESTAMP,
        loaddt DATE
    )
    PARTITION BY _PARTITIONDATE;

    INSERT INTO `curatedds.trans_mobile_autopart_2021` (txnno, dt, ts, geo_coordinate, net, provider, activity, postal_code, town_name, loadts, loaddt)
    SELECT txnno, CAST(dt AS DATE) AS dt, TIMESTAMP(CONCAT(dt, ' ', hour)) AS ts, ST_GEOGPOINT(long, lat) AS geo_coordinate, net, provider, activity, postal_code, town_name, loadts, DATE(loadts) AS loaddt
    FROM `rawds.trans_mobile_channel`
    WHERE EXTRACT(YEAR FROM CAST(dt AS DATE)) = 2021;

    CREATE TABLE IF NOT EXISTS curatedds.trans_mobile_autopart_2022 (
        txnno NUMERIC,
        dt DATE,
        ts TIMESTAMP,
        geo_coordinate GEOGRAPHY,
        net STRING,
        provider STRING,
        activity STRING,
        postal_code INT64,
        town_name STRING,
        loadts TIMESTAMP,
        loaddt DATE
    )
    PARTITION BY _PARTITIONDATE;

    INSERT INTO curatedds.trans_mobile_autopart_2022 (txnno, dt, ts, geo_coordinate, net, provider, activity, postal_code, town_name, loadts, loaddt)
    SELECT txnno, CAST(dt AS DATE) AS dt, TIMESTAMP(CONCAT(dt, ' ', hour)) AS ts, ST_GEOGPOINT(long, lat) AS geo_coordinate, net, provider, activity, postal_code, town_name, loadts, DATE(loadts) AS loaddt
    FROM `rawds.trans_mobile_channel`
    WHERE EXTRACT(YEAR FROM CAST(dt AS DATE)) = 2022;

    CREATE TABLE IF NOT EXISTS curatedds.trans_mobile_autopart_2023 (
        txnno NUMERIC,
        dt DATE,
        ts TIMESTAMP,
        geo_coordinate GEOGRAPHY,
        net STRING,
        provider STRING,
        activity STRING,
        postal_code INT64,
        town_name STRING,
        loadts TIMESTAMP,
        loaddt DATE
    )
    PARTITION BY _PARTITIONDATE;

    INSERT INTO curatedds.trans_mobile_autopart_2023 (txnno, dt, ts, geo_coordinate, net, provider, activity, postal_code, town_name, loadts, loaddt)
    SELECT txnno, CAST(dt AS DATE) AS dt, TIMESTAMP(CONCAT(dt, ' ', hour)) AS ts, ST_GEOGPOINT(long, lat) AS geo_coordinate, net, provider, activity, postal_code, town_name, loadts, DATE(loadts) AS loaddt
    FROM (
        SELECT CASE WHEN CAST(dt AS DATE) > CURRENT_DATE() THEN CURRENT_DATE() ELSE dt END AS dt, * EXCEPT(dt)
        FROM `rawds.trans_mobile_channel`
    ) t
    WHERE EXTRACT(YEAR FROM CAST(dt AS DATE)) = 2023;

END;

END;