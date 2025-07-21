-- Variable Declaration and Initialization
-- In BigQuery, DECLARE and SET are used to define and assign values to variables,
-- similar to variable assignment in procedural languages (e.g., load_ts = datetime.now() in Python).
DECLARE loadts TIMESTAMP;
SET loadts = (SELECT CURRENT_TIMESTAMP());
-- Example value: loadts = 2025-07-21 08:04:11 PM IST (reflecting current timestamp)

BEGIN

    -- Section: Schema Creation and Table Definitions
    ----------------------------------------------------------
    -- This section defines the `curatedds` schema and the various curated tables.
    -- It also includes comments on full data refresh strategies, BigQuery/Hive differences,
    -- and partitioning/clustering concepts.

    -- Strategies for Complete Refresh (delete all target data and reload with the entire source data):
    -- 1. Truncate Table and Insert: Deletes only data, preserving structure, then inserts. (Preference: 1st, generally faster for full clears)
    --    Example: `TRUNCATE TABLE my_dataset.my_table; INSERT INTO my_dataset.my_table SELECT * FROM raw_data;`
    -- 2. Drop and Recreate Table (`CREATE OR REPLACE TABLE`): Deletes both table structure and data, then recreates. (Preference: 2nd, useful for schema changes)
    --    Example: `CREATE OR REPLACE TABLE my_dataset.my_table (id INT64, name STRING);`
    -- 3. Delete with Filters and Insert: Deletes specific portions or all data, then inserts. (Preference: 3rd, slower if deleting all data without filters).
    --    Example: `DELETE FROM my_dataset.my_table WHERE date = '2025-07-21'; INSERT INTO my_dataset.my_table SELECT * FROM new_data WHERE date = '2025-07-21';`

    -- Note on INSERT OVERWRITE:
    -- BigQuery does NOT support `INSERT OVERWRITE INTO TABLE` like Hive.
    -- As a workaround, explicit `DELETE` followed by `INSERT` is used for idempotent loads or full refreshes.

    -- Interview Question 1: Difference between DROP, TRUNCATE, and DELETE.
    -- DROP: DDL (Data Definition Language). Deletes table structure and all data. Permanent and irreversible.
    --       Example: `DROP TABLE my_dataset.old_table;`
    -- TRUNCATE: DDL. Deletes all data quickly without logging individual row deletions. Preserves table structure. Faster than DELETE for full table clear.
    --       Example: `TRUNCATE TABLE my_dataset.temp_table;`
    -- DELETE: DML (Data Manipulation Language). Deletes data based on a WHERE clause. Slower for full table deletion as it logs each row deletion.
    --       Example: `DELETE FROM my_dataset.transactions WHERE amount < 10;`
    --       In BigQuery, `DELETE FROM table;` (without a WHERE clause) is not typically allowed; it requires a `WHERE` clause for safety, or `TRUNCATE` for full table clears.

    -- Interview Question 2: Key Differences between Hive & BigQuery (for migration/modernization challenges)
    -- 1. DML Operations:
    --    - BigQuery supports `DELETE`, `UPDATE`, `MERGE` directly.
    --    - Hive historically did not support direct `DELETE`/`UPDATE`/`MERGE` without workarounds (e.g., creating a new table from selected data, or requiring ACID tables for direct DML).
    -- 2. Insert Overwrite:
    --    - BigQuery: Does NOT support `INSERT OVERWRITE` for tables/partitions.
    --    - Hive: *Does* support `INSERT OVERWRITE TABLE` and `INSERT OVERWRITE PARTITION`.
    --      Example (Hive): `INSERT OVERWRITE TABLE my_hive_table PARTITION (dt='2025-07-21') SELECT col1, col2 FROM staging_table;`
    -- 3. Partitioning:
    --    - Purpose: Stores data internally into given partition folders to improve filter performance by avoiding Full Table Scans (FTS).
    --    - BigQuery: Supports only ONE partition column per table; data type can be `DATE`, `TIMESTAMP`, `DATETIME`, or `INTEGER`. Max 4000 unique partition values.
    --      Example (BQ Partition): `CREATE TABLE my_table (id INT64, event_date DATE) PARTITION BY event_date;`
    --    - Hive: Supports multi-level (multiple) partition columns; supports `STRING` and other data types for partitions.
    --      Example (Hive Multi-Partition): `CREATE TABLE my_hive_table (id INT) PARTITIONED BY (load_dt DATE, category STRING);`
    --    - Syntax: BigQuery partition columns must be part of the table schema (`CREATE TABLE tbl1(id INTEGER, load_dt DATE) PARTITION BY load_dt;`). Hive partition columns are defined separately (`CREATE TABLE tbl1(id INT) PARTITIONED BY (load_dt DATE);`).
    --    - Management: BigQuery simplifies partition management (e.g., no `MSCK REPAIR TABLE` needed as metadata and data layers are integrated, unlike Hive where they are separated).
    --    - Applicability: Best for low-cardinality columns (e.g., `date`, `age range`).

    -- *** Important - Challenge in migrating from Hive to BQ:
    -- Since `INSERT OVERWRITE` into table option is not supported in BQ as like hive, we can't do insert overwrite partition also.
    -- As a workaround, we have to delete the data for the specific partition separately, then load into the partition table.
    -- Example Workaround: `DELETE FROM my_bq_partitioned_table WHERE loaddt = CURRENT_DATE(); INSERT INTO my_bq_partitioned_table SELECT ... WHERE some_date = CURRENT_DATE();`

    CREATE SCHEMA IF NOT EXISTS curatedds OPTIONS(location='us');

    CREATE OR REPLACE TABLE curatedds.consumer_full_load(
        custno INT64,
        fullname STRING,
        age INT64,
        yearofbirth INTEGER,
        profession STRING,
        loadts TIMESTAMP,
        loaddt DATE);
    -- The above query will drop the table if it exists and then recreate it.
    -- Equivalent to:
    -- TRUNCATE TABLE curatedds.consumer_full_load;
    -- DELETE FROM curatedds.consumer_full_load WHERE filters....;

    -- Create Partitioned Table for Online Transactions
    -- Partitions can be created only on low-cardinality Date & Number columns. Maximum number of partitions can be only 4000.
    -- Example of multi-level partitioning (not supported in BigQuery): `create table if not exists tablename(id int,name string) partitioned by (loaddt date,category string)`
    CREATE TABLE IF NOT EXISTS curatedds.trans_online_part
    (
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
    OPTIONS (require_partition_filter = TRUE);
    -- `require_partition_filter` will enforce a mandatory partition filter to be used when querying the table,
    -- preventing full table scans and improving cost efficiency.
    -- Example query with filter: `SELECT * FROM curatedds.trans_online_part WHERE loaddt = '2025-07-21';`

    -- Interview Question 3: When do we go for Clustering in BQ?
    -- For join and filter performance improvement.
    -- BigQuery Clustering Concepts:
    -- - Maximum cluster by columns can be only 4 in BQ (Hive bucketing can support more conceptually).
    -- - Difference between Hive bucketing and BQ clustering? Both are technically similar (sorting and grouping/bucketing of data), but syntax varies.
    -- - Clustering columns order of defining is very important for performance.
    -- - Bigquery supports clustering (sorting and grouping/bucketing of the clustered columns) to improve filter and join performances.
    --   - BQ syntax: `CLUSTER BY col1, col2` (up to max 4 columns).
    --     Example: `CLUSTER BY order_date, customer_id`
    --   - Hive syntax is: `CLUSTERED BY col1, col2... INTO N BUCKETS SORT BY col1, col2`.
    -- - Clustering can be applied on both low/high cardinal columns like (date, age, custno) - start with low and lead to high cardinal.
    -- - In clustering, the order of columns is important.

    -- Create Partitioned and Clustered Table for POS Transactions
    CREATE TABLE IF NOT EXISTS curatedds.trans_pos_part_cluster
    (
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
    CLUSTER BY loaddt, custno, txnno -- Clustered by load date, customer number, and transaction number
    OPTIONS (description = 'Point of sale table with partition and clusters');

END;

-- Section: Full Refresh Load for Consumer Data
----------------------------------------------
-- This block processes raw consumer data and loads it into the curated `consumer_full_load` table using a full refresh strategy.
BEGIN
    -- Interview Question 4: Difference between delete, truncate and drop+recreate (create or replace table).
    -- DELETE: DML. It helps us delete a portion of data applying a WHERE clause. In BQ it is mandatory to use where clause.
    --         Example: `DELETE FROM my_table WHERE status = 'old';`
    -- TRUNCATE: DDL. It helps us delete the entire data (without affecting the structure) without applying any filters (faster than delete).
    --           Example: `TRUNCATE TABLE staging_data;`
    -- DROP: DDL. It helps us drop the table structure and data also.
    --       Example: `DROP TABLE my_old_table;`

    -- DATA MUNGING:
    -- - Cleansing: e.g., De-duplication.
    -- - Scrubbing: e.g., Replacing of null with some values.
    -- - Curation (Business Logic): e.g., Concatenation.
    -- - Data Enrichment: e.g., Deriving a column from existing or new columns.

    -- Inline View (FROM clause subquery):
    -- - Good for nominal data size, but not good for holding large volume of data in memory. If volume is large, go with a temporary view/table.
    -- - `QUALIFY` function (commented out) can be an alternative to inline view for filtering based on rank/partitioning functions.
    --   Example (using QUALIFY): `SELECT custid, ... FROM rawds.consumer QUALIFY ROW_NUMBER() OVER (PARTITION BY custid ORDER BY age DESC) = 1;`
    -- This is a Slowly Changing Dimension (SCD) Type 1 Load (Since we are not maintaining History).

    /*
    #Subquery Concepts (for interview context):
    -- Different types of subqueries: FROM clause subquery (inline view), scalar subquery, multi-row subquery.
    -- Multi-columns subqueries (e.g., `WHERE (col1, col2) IN (...)`) are not directly supported in BQ.
    --   Workaround: `WHERE CONCAT(col1, col2) IN (SELECT DISTINCT CONCAT(col1, col2) FROM ...);`
    -- Correlated subquery - Definition: For every row execution of the parent query, the subquery will be executed and the response of the child will be used by the parent for the final result.
    -- Analogy example: "Irfan taking his daughter to chocolate shop, asking her to choose any dark chocolates. Daughter selected the chocolate. Irfan bought that from the store."
    -- Example (correlated subquery):
    -- `SELECT * FROM rawds.consumer irfan WHERE EXISTS (SELECT 'yes' FROM rawds.consumer daughter WHERE irfan.custid=daughter.custid AND irfan.age=daughter.age AND daughter.profession IS NULL);`
    -- Scalar subquery in SELECT:
    -- `SELECT *, (SELECT MAX(age) FROM rawds.consumer) max_age FROM rawds.consumer irfan LIMIT 10;`
    -- Subqueries can generally be used in SELECT, FROM, WHERE, HAVING clauses (other than GROUP BY and ORDER BY directly).
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
                ROW_NUMBER() OVER (PARTITION BY custid ORDER BY age DESC) AS rnk -- Deduplicates, picking latest by age
            FROM rawds.consumer
            -- qualify rnk=1 #will help us avoid inline view
        ) AS inline_view
        WHERE rnk = 1
    ) AS dedup;

END;

-- Section: Online Transactions Curation and Load
-------------------------------------------------
-- This block processes semi-structured online transaction data, curates it,
-- and loads it into a partitioned BigQuery table using a temporary table.
BEGIN
    -- Interview Question 5: BigQuery Temporary Tables
    -- - Definition: A session-scoped table that stores data in the underlying Colossus storage, not in memory like inline views.
    -- - Persistence: Persists only for the duration of the session (such as a BEGIN ... END block or script), or up to 24 hours if accessed via job information.
    -- - Use Cases: Use when working with large intermediate results that may not fit well in memory if handled with inline views or CTEs.
    -- - Benefits: Improves performance, reusability, and makes complex queries easier to structure by storing intermediate results that can be reused multiple times within the same script or session.
    -- - Additional Features for interview purpose:
    --   - Auto-Cleanup: Automatically cleaned up within the session or after 1 day expiration.
    --   - Faster for Intermediates: Since it uses Colossus storage.
    --   - Cost-Efficient: No long-term storage costs (only 1 day max).
    --   - Session-Scoped: No conflict with other sessions.
    --   - Helps in cases where Common Table Expressions (CTEs) or Inline view might not perform well for large data.

    -- Curation Logic:
    -- - Converting from Semistructured to structured using `UNNEST` function (equivalent to `EXPLODE` in Hive).
    -- - Generating surrogate key (`ABS(FARM_FINGERPRINT(GENERATE_UUID()))`) for a unique identifier.
    -- - Splitting of columns by extracting the string and number portion from the product column.
    --   Example: `SELECT REGEXP_REPLACE('4B2A', '[^0-9]','')` would return '42'.
    -- - Standardization: The `CASE WHEN COALESCE(TRIM(REGEXP_REPLACE(LOWER(prod_exploded.productid),'[^a-z]','')),'')='' THEN 'na'` logic will convert blank spaces/nulls/number-only strings to 'na', otherwise, it allows only the string portion to the target.
    -- - Adding `loadts` and `loaddt` columns for load data.

    /* How the unnesting is happening / semistructured to structured conversion using unnest (equivalent to explode in hive)
    Example JSON Input:
    {"orderId":"1","storeLocation":"Newyork","customerId" : "4000011",
    "products":[{"productId":"1234","productCategory":"Laptop","productName" : "HP Pavilion","productPrice": "540"},
                {"productId":"421","productCategory":"Mobile","productName" : "Samsung S22","productPrice": "240"}]}

    Conceptual Nested (semi structured) View:
    orderId,storeLocation,customerId,products (array of structs)
                                   productId,productCategory,productName,productPrice
    1      ,Newyork,      4000011   ,1234,     Laptop,         HP Pavilion,540
                                     421,      Mobile,         Samsung S22,240

    Un Nested (structured) Output after UNNEST(p.products):
    orderId,storeLocation,customerId,products.productId,products.productCategory,products.productName,products.productPrice
    1      ,Newyork,      4000011   ,1234,     Laptop,         HP Pavilion,540
    1      ,Newyork,      4000011   ,421,      Mobile,         Samsung S22,240
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


    -- Example for better understanding of Unnesting:
    -- Nested Sample: `4000015,[{Table,Furniture,440,34P},{Chair,Furniture,440,42A}]`
    -- Unnested Output:
    -- `4000015,Table,Furniture,440,34P`
    -- `4000015,Chair,Furniture,440,42A`

    -- Idempotent Delete for Job Restartability/Rerun:
    -- This `DELETE` statement based on load date is placed at the beginning of the each query file
    -- to ensure job restartability and proper re-runs.
    -- This is crucial as BigQuery does not support `INSERT OVERWRITE PARTITION` like Hive.
    DELETE FROM curatedds.trans_online_part WHERE loaddt = DATE(loadts);

    -- Loading of partitioned table in BigQuery:
    -- As like Hive, we don't need `partition()` clause, and we can't use `INSERT OVERWRITE`.
    -- Example (Hive): `INSERT OVERWRITE TABLE curatedds.trans_online_part PARTITION(loaddt) SELECT * FROM online_trans_view;`
    -- In BQ, we have to delete the partition explicitly, then load the data if any data is going to be repeated.
    INSERT INTO curatedds.trans_online_part SELECT * FROM online_trans_view;

    -- To understand reusability feature of the temp table:
    -- How to create a table structure from another table without loading data.
    CREATE TABLE IF NOT EXISTS curatedds.trans_online_part_furniture AS SELECT * FROM online_trans_view WHERE 1 = 2; -- Creates table structure only (WHERE 1=2 ensures no data is loaded)
    TRUNCATE TABLE curatedds.trans_online_part_furniture; -- Clears any data if table already existed from a previous run
    INSERT INTO curatedds.trans_online_part_furniture SELECT * FROM online_trans_view WHERE productcategory='Furniture';
END;

-- Section: POS Transactions Curation and Load
---------------------------------------------
-- This block curates raw point-of-sale (POS) transaction data and loads it into a partitioned and clustered table.
BEGIN
    -- CURATION logic:
    -- - Date format conversion and casting of string to date.
    --   Example: `PARSE_DATE('%m-%d-%Y', '07-21-2025')` would convert the string to a DATE type.
    -- - Data conversion: `COALESCE(product, 'NA')` handles null product values, replacing them with 'NA'.
    --   Example: If `product` is NULL, it becomes 'NA'; otherwise, its original value is used.
    -- - Column selection: `* EXCEPT(txnno, txndt, custno, amt, product)` selects all columns from the source
    --   except the ones explicitly transformed or not needed, while maintaining an ordered selection.
    INSERT INTO curatedds.trans_pos_part_cluster
    SELECT
        txnno,
        PARSE_DATE('%m-%d-%Y', txndt) AS txndt,
        custno,
        amt,
        COALESCE(product, 'NA') AS product,
        * EXCEPT(txnno, txndt, custno, amt, product),
        loadts,
        DATE(loadts) AS loaddt
    FROM `rawds.trans_pos`;
END;

-- Section: Mobile Transactions Curation and Load (Auto-Partitioned by Year)
---------------------------------------------------------------------------
-- This block processes raw mobile transaction data and loads it into year-specific, auto-partitioned tables.
BEGIN
    -- Curation Logic & Design Choices:
    -- - Storing data into 3 year-specific tables (`_2021`, `_2022`, `_2023`) for:
    --   - Improved performance: By picking only data from the given year table, queries scan smaller data sets.
    --   - Cost Reduction: By storing older data in BigQuery's long-term (cheaper) storage tier, as it may be accessed less frequently.
    -- - Drawback (addressed by BQ features): While it means potentially combining tables using `UNION ALL` or `UNION DISTINCT` to get all table data in one shot, BigQuery provides excellent wildcard table queries (e.g., `SELECT * FROM `dataset.tablename*`` to query all years, or `SELECT * FROM `dataset.tablename_202*`` for a specific decade).
    -- - Merging of date and time columns to a `TIMESTAMP` column.
    --   Example: `TIMESTAMP(CONCAT('2025-07-21',' ','10:30:00'))` would result in a `TIMESTAMP` value.
    -- - Merging of longitude and latitude to `GEOGRAPHY` data type (`ST_GEOGPOINT`) for geospatial analysis and plotting on maps.
    --   Example: `ST_GEOGPOINT(-74.0060, 40.7128)` (for New York City coordinates).

    -- Use Cases highlighted:
    -- - Auto-partitioning (`_PARTITIONDATE`): BigQuery automatically creates and maintains partitions based on the internal clock's load date. This simplifies partition management and is useful even if your dataset doesn't explicitly have a date column for partitioning.
    -- - `GEOGRAPHY` datatype: Used to hold GPS coordinates (longitude, latitude).
    -- - Wildcard table queries for accessing data across segregated tables.
    -- - Table segregation for performance and cost optimization.
    -- - For current year (2023) data, handling future dates in the source by converting them to `CURRENT_DATE()` using an inline view/FROM clause subquery to correct data quality issues.
    --   Example: If source has `dt = '2026-01-01'` in a 2023 load, it will be corrected to `CURRENT_DATE()` (e.g., '2025-07-21').

    -- Create and Load 2021 Mobile Transactions Table (Auto-Partitioned by Ingestion Date)
    CREATE TABLE IF NOT EXISTS curatedds.trans_mobile_autopart_2021
    (
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
    SELECT
        txnno,
        CAST(dt AS DATE) dt,
        TIMESTAMP(CONCAT(dt, ' ', hour)) AS ts,
        ST_GEOGPOINT(long, lat) AS geo_coordinate,
        net,
        provider,
        activity,
        postal_code,
        town_name,
        loadts,
        DATE(loadts) AS loaddt
    FROM `rawds.trans_mobile_channel`
    WHERE EXTRACT(YEAR FROM CAST(dt AS DATE)) = 2021;


    -- Create and Load 2022 Mobile Transactions Table (Auto-Partitioned by Ingestion Date)
    CREATE TABLE IF NOT EXISTS curatedds.trans_mobile_autopart_2022
    (
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
    SELECT
        txnno,
        CAST(dt AS DATE) dt,
        TIMESTAMP(CONCAT(dt, ' ', hour)) AS ts,
        ST_GEOGPOINT(long, lat) AS geo_coordinate,
        net,
        provider,
        activity,
        postal_code,
        town_name,
        loadts,
        DATE(loadts) AS loaddt
    FROM `rawds.trans_mobile_channel`
    WHERE EXTRACT(YEAR FROM CAST(dt AS DATE)) = 2022;

    -- Create and Load 2023 Mobile Transactions Table (Auto-Partitioned by Ingestion Date, with Date Correction)
    CREATE TABLE IF NOT EXISTS curatedds.trans_mobile_autopart_2023
    (
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
    SELECT
        txnno,
        CAST(dt AS DATE) dt,
        TIMESTAMP(CONCAT(dt, ' ', hour)) AS ts,
        ST_GEOGPOINT(long, lat) AS geo_coordinate,
        net,
        provider,
        activity,
        postal_code,
        town_name,
        loadts,
        DATE(loadts) AS loaddt
    FROM (SELECT CASE WHEN CAST(dt AS DATE) > CURRENT_DATE() THEN CURRENT_DATE() ELSE dt END AS dt, * EXCEPT(dt) FROM `rawds.trans_mobile_channel`) t
    WHERE EXTRACT(YEAR FROM CAST(dt AS DATE)) = 2023;
END;