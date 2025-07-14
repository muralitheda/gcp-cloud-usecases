-- Begin block for Curated Data Schema and Table Creation
BEGIN
    -- Strategies for Complete Refresh (delete entire target data and reload with entire source data):
    -- Option 1: DROP and re-CREATE the table (deletes both structure and data).
    -- Option 2: TRUNCATE table and then INSERT (deletes only data).
    -- Option 3: DELETE rows based on filters and then INSERT (deletes portion/entire data).

    -- Interview Questions:
    -- - Difference between DROP (CREATE OR REPLACE TABLE), TRUNCATE, and DELETE in BigQuery.
    -- - BigQuery vs. Hive:
    --   - BigQuery does not support `INSERT OVERWRITE` directly like Hive.
    --   - BigQuery supports `DELETE` directly; Hive typically requires workarounds.

    -- Create curatedds schema if it does not exist, with location 'us'
    CREATE SCHEMA IF NOT EXISTS curatedds
    OPTIONS(location='us');

    -- Create or replace curatedds.consumer_full_load table
    CREATE OR REPLACE TABLE curatedds.consumer_full_load (
        custno      INT64,
        fullname    STRING,
        age         INT64,
        yearofbirth INT64,
        profession  STRING,
        loadts      TIMESTAMP,
        loaddt      DATE
    );

    -- TRUNCATE TABLE curatedds.consumer_full_load; -- Uncomment to truncate before load
    -- DELETE FROM curatedds.consumer_full_load WHERE filters...; -- Uncomment to delete specific rows

    -- BigQuery Partitioning Notes:
    -- - Supports only DATE or INTEGER columns for partitioning (unlike Hive which supports STRING).
    -- - Supports only single partitioning columns (no multi-level partitions, unlike Hive).
    -- - Syntax: Partition column can be defined in both CREATE TABLE column list and PARTITION BY clause.
    -- - Partition management is simplified; no `MSCK REPAIR TABLE` or refreshing needed like Hive.
    -- - Partitions can be created only on low cardinality DATE & NUMBER columns.
    -- - Maximum number of partitions is 4000.

    -- Important - Challenge in migrating from Hive to BigQuery:
    -- Since `INSERT OVERWRITE INTO TABLE` is not supported in BigQuery, we cannot directly overwrite partitions.
    -- Workaround: Explicitly `DELETE` data for the relevant partition(s) and then `INSERT` new data.
    -- Example: `DELETE FROM table WHERE loaddt = DATE(loadts_variable);`

    -- Create curatedds.trans_online_part table with partitioning
    CREATE TABLE IF NOT EXISTS curatedds.trans_online_part (
        transsk         NUMERIC,
        customerid      INT64,
        productname     STRING,
        productcategory STRING,
        productprice    INT64,
        productidint    INT64,
        prodidstr       STRING,
        loadts          TIMESTAMP,
        loaddt          DATE
    )
    PARTITION BY loaddt
    OPTIONS (require_partition_filter = FALSE);
    -- `require_partition_filter` enforces mandatory partition filtering for table users.

    -- BigQuery Clustering Notes:
    -- - Supports clustering (sorting and grouping/bucketing of clustered columns) to improve filter and join performance.
    -- - Conceptually similar to Hive bucketing, but syntax varies.
    -- - BQ Syntax: `CLUSTER BY col1, col2...`
    -- - Hive Syntax: `CLUSTERED BY col1, col2... INTO N BUCKETS SORT BY col1, col2`

    -- Create curatedds.trans_pos_part_cluster table with partitioning and clustering
    CREATE TABLE IF NOT EXISTS curatedds.trans_pos_part_cluster (
        txnno    NUMERIC,
        txndt    DATE,
        custno   INT64,
        amount   FLOAT64,
        category STRING,
        product  STRING,
        city     STRING,
        state    STRING,
        spendby  STRING,
        loadts   TIMESTAMP,
        loaddt   DATE
    )
    PARTITION BY loaddt
    CLUSTER BY loaddt, custno, txnno
    OPTIONS (description = 'point of sale table with partition and clusters');

    -- Block 1 for loading raw consumer data to the curated consumer table (Full Refresh)
    -- This block uses an inline view (FROM clause subquery).
    -- Inline views are suitable for nominal data sizes; for large volumes, consider temporary tables.
    BEGIN
        -- TRUNCATE TABLE curatedds.consumer_full_load; -- Uncomment to truncate before load

        -- Curation Steps:
        -- - Cleansing: Deduplication
        -- - Scrubbing: Replacing NULLs with default values
        -- - Curation (Business Logic): Concatenating first and last names
        -- - Data Enrichment: Deriving 'yearofbirth' from 'age'

        INSERT INTO curatedds.consumer_full_load
        SELECT
            custid,
            CONCAT(firstname, ' ', lastname) AS fullname,
            age,
            EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE, INTERVAL -age * 12 MONTH)) AS yearofbirth,
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
                    ROW_NUMBER() OVER(PARTITION BY custid ORDER BY age DESC) AS rnk
                FROM rawds.consumer
            ) AS inline_view
            WHERE rnk = 1
        ) AS dedup;
    END;

    -- Block for Online Transactions Curation and Loading
    BEGIN
        -- Temporary Table Concept in BigQuery:
        -- - Temp tables are session-scoped and hold data in the underlying Colossus store (not memory like inline views).
        -- - They exist only for the duration of the `BEGIN...END` block.
        -- - Useful for reusing query results multiple times within a block and for handling very large data volumes that might cause performance issues with inline views.

        -- Curation Logic:
        -- - Converting semi-structured data to structured using `UNNEST` (equivalent to Hive's `EXPLODE`).
        -- - Generating a surrogate key (`transk`) using `FARM_FINGERPRINT(GENERATE_UUID())`.
        -- - Splitting columns by extracting string and number portions from the product column.
        -- - Standardizing product IDs (e.g., converting blanks/nulls/numbers-only to 'na').
        -- - Adding `loadts` and `loaddt` columns.

        CREATE OR REPLACE TEMP TABLE online_trans_view AS
        SELECT
            ABS(FARM_FINGERPRINT(GENERATE_UUID())) AS transk,
            customerid,
            prod_exploded.productname,
            prod_exploded.productcategory,
            prod_exploded.productprice,
            CAST(REGEXP_REPLACE(prod_exploded.productid, '[^0-9]', '') AS INT64) AS prodidint,
            CASE
                WHEN COALESCE(TRIM(REGEXP_REPLACE(LOWER(prod_exploded.productid), '[^a-z]', '')), '') = '' THEN 'na'
                ELSE REGEXP_REPLACE(LOWER(prod_exploded.productid), '[^a-z]', '')
            END AS prodidstr,
            loadts,
            DATE(loadts) AS loaddt
        FROM `rawds.trans_online` AS p, UNNEST (products) AS prod_exploded;

        -- Example of Nested vs. Unnested data for clarity:
        -- Nested: 4000015,[{Table,Furniture,440,34P},{Chair,Furniture,440,42A}]
        -- Unnested:
        -- 4000015,Table,Furniture,440,34P
        -- 4000015,Chair,Furniture,440,42A

        -- Delete statement for job restartability/rerun of jobs (deletes data for the current load date)
        DELETE FROM curatedds.trans_online_part WHERE loaddt = DATE(loadts);

        -- Unlike Hive, BigQuery does not use `PARTITION()` in `INSERT` statements and does not support `INSERT OVERWRITE` for partitions.
        -- In BigQuery, if data needs to be replaced in a partition, you must explicitly delete the old data for that partition before inserting.
        -- Loading data into the partitioned table:
        INSERT INTO curatedds.trans_online_part SELECT * FROM online_trans_view;

        -- Example of temp table reusability: Creating a table structure from another table without loading data.
        CREATE TABLE IF NOT EXISTS curatedds.trans_online_part_furniture AS SELECT * FROM online_trans_view WHERE 1 = 2;
        TRUNCATE TABLE curatedds.trans_online_part_furniture;
        INSERT INTO curatedds.trans_online_part_furniture SELECT * FROM online_trans_view WHERE productcategory = 'Furniture';
    END;

    -- Block for POS Transactions Curation and Loading
    BEGIN
        -- Curation Logic:
        -- - Date format conversion and casting of string to date.
        -- - Data conversion: setting 'product' to 'NA' if NULL.
        -- - Selecting specific columns and using `EXCEPT` to exclude original columns after transformation.

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

    -- Block for Mobile Transactions Curation and Loading
    BEGIN
        -- Curation Logic & Use Cases:
        -- - Storing data into year-specific tables for performance and cost optimization.
        --   - Improves performance by querying only relevant year's data.
        --   - Reduces cost by leveraging BigQuery's storage tiers (active vs. long-term) for older data.
        -- - Wildcard table queries (`SELECT * FROM `dataset.tablename*``) can combine data from multiple year tables.
        -- - Merging date and time columns into a single `TIMESTAMP` column.
        -- - Merging longitude and latitude into a `GEOGRAPHY` data type for geospatial plotting.
        -- - Auto-partitioning (`_PARTITIONDATE`): BigQuery automatically creates and maintains partitions based on load date.
        -- - Handling data issues: Correcting future dates to `CURRENT_DATE()` using an inline view.

        -- Create table for 2021 mobile transactions with auto-partitioning
        CREATE TABLE IF NOT EXISTS curatedds.trans_mobile_autopart_2021 (
            txnno          NUMERIC,
            dt             DATE,
            ts             TIMESTAMP,
            geo_coordinate GEOGRAPHY,
            net            STRING,
            provider       STRING,
            activity       STRING,
            postal_code    INT64,
            town_name      STRING,
            loadts         TIMESTAMP,
            loaddt         DATE
        )
        PARTITION BY _PARTITIONDATE;

        INSERT INTO `curatedds.trans_mobile_autopart_2021` (txnno, dt, ts, geo_coordinate, net, provider, activity, postal_code, town_name, loadts, loaddt)
        SELECT
            txnno,
            CAST(dt AS DATE) AS dt,
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

        -- Create table for 2022 mobile transactions with auto-partitioning
        CREATE TABLE IF NOT EXISTS curatedds.trans_mobile_autopart_2022 (
            txnno          NUMERIC,
            dt             DATE,
            ts             TIMESTAMP,
            geo_coordinate GEOGRAPHY,
            net            STRING,
            provider       STRING,
            activity       STRING,
            postal_code    INT64,
            town_name      STRING,
            loadts         TIMESTAMP,
            loaddt         DATE
        )
        PARTITION BY _PARTITIONDATE;

        INSERT INTO curatedds.trans_mobile_autopart_2022 (txnno, dt, ts, geo_coordinate, net, provider, activity, postal_code, town_name, loadts, loaddt)
        SELECT
            txnno,
            CAST(dt AS DATE) AS dt,
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

        -- Create table for 2023 mobile transactions with auto-partitioning
        CREATE TABLE IF NOT EXISTS curatedds.trans_mobile_autopart_2023 (
            txnno          NUMERIC,
            dt             DATE,
            ts             TIMESTAMP,
            geo_coordinate GEOGRAPHY,
            net            STRING,
            provider       STRING,
            activity       STRING,
            postal_code    INT64,
            town_name      STRING,
            loadts         TIMESTAMP,
            loaddt         DATE
        )
        PARTITION BY _PARTITIONDATE;

        INSERT INTO curatedds.trans_mobile_autopart_2023 (txnno, dt, ts, geo_coordinate, net, provider, activity, postal_code, town_name, loadts, loaddt)
        SELECT
            txnno,
            CAST(dt AS DATE) AS dt,
            TIMESTAMP(CONCAT(dt, ' ', hour)) AS ts,
            ST_GEOGPOINT(long, lat) AS geo_coordinate,
            net,
            provider,
            activity,
            postal_code,
            town_name,
            loadts,
            DATE(loadts) AS loaddt
        FROM (
            SELECT
                CASE
                    WHEN CAST(dt AS DATE) > CURRENT_DATE() THEN CURRENT_DATE()
                    ELSE dt
                END AS dt,
                * EXCEPT(dt)
            FROM `rawds.trans_mobile_channel`
        ) AS t
        WHERE EXTRACT(YEAR FROM CAST(dt AS DATE)) = 2023;
    END;
END;
