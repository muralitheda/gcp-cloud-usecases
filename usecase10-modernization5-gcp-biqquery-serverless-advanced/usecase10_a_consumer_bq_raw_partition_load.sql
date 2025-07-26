/*
Author: muralitheda
Date: July 26, 2025
Description: This script demonstrates key data warehousing and data lake concepts
             including Data Lakes, Lakehouses, Google Cloud's BigLake, dynamic SQL, Change Data Capture (CDC) for Source
             and Slowly Changing Dimensions (SCD) Type 1 and Type 2 implementations for Target.
*/

/*
Concepts Added in this 10.A usecase:

What is the difference between:
- Datalake: A scalable file system storage layer that accepts any type/volume of data (raw, unstructured, semi-structured, structured).
- Lakehouse (managed/native table): An architecture that enables data warehousing capabilities on top of a data lake by "owning" the data within the data lake. It combines the flexibility of data lakes with the structure and management features of data warehouses (e.g., ACID transactions, schema enforcement).
- BigLake (External Table): A technology (specifically Google Cloud's) that enables data warehousing capabilities on top of a data lake by "referring" to the data in the data lake, rather than moving or owning it directly within the warehouse.

External table restrictions (cloud side):
- Data will not be stored in Colossus/Jupyter network (BigQuery's native storage).
- Cannot be truncated, updated/deleted directly via BigQuery DML.
- Data will not be dropped from the underlying storage if the external table is dropped.

This script demonstrates the following concepts:
1. BigLake (External Table Concept)
2. Dynamic SQL, Metadata/parameter driven approach
3. Merge Statement
4. Declare, Begin End block, Exception Handling... (though explicit exception handling is not shown in this snippet)
5. CDC/Incremental(delta) data collection & SCD Type1 (no history) & Type2 load (history is there)
*/

DECLARE v_uri STRING;
DECLARE v_datadt DATE;
DECLARE v_datadtraw STRING;
DECLARE dynamicsql STRING;

-- Example setup for metadata-driven approach (uncomment and run if 'curatedds.etl_meta' table doesn't exist)
-- create table curatedds.etl_meta (id int64,rulesql string);
-- insert into curatedds.etl_meta values(3,"gs://iz-cloud-training-project-bucket/data/custs_header_20250701");

SET v_uri = (SELECT rulesql FROM curatedds.etl_meta WHERE id = 3); -- metadata driven approach
-- Alternatively, set v_uri directly:
-- SET v_uri = 'gs://iz-cloud-training-project-bucket/data/custs_header_20250701';

SET v_datadtraw = (SELECT RIGHT(v_uri, 8)); -- Extracts the date part from the URI
SET v_datadt = (SELECT PARSE_DATE('%Y%m%d', v_datadtraw)); -- Converts the extracted string to a DATE type

-- CDC (Change Data Capture based on the date parameter suffixed in the filename)
-- Constructing the dynamic SQL query to create or replace the BigLake external table.
-- This combines three different string parts, inserting the `v_uri` in the middle.
SET dynamicsql = CONCAT('CREATE OR REPLACE EXTERNAL TABLE rawds.cust_ext (
    custno INT64,
    firstname STRING,
    lastname STRING,
    age INT64,
    profession STRING,
    upd_ts TIMESTAMP
)
OPTIONS (
    format = "CSV",
    uris = ["', v_uri, '"],
    max_bad_records = 2,
    skip_leading_rows = 1
)');

-- Alternative way to format dynamic SQL using FORMAT function (commented out)
-- SET dynamicsql = 'CREATE OR REPLACE EXTERNAL TABLE rawds.cust_ext ( custno INT64,firstname STRING,lastname STRING,age INT64,profession STRING,upd_ts timestamp) OPTIONS (   format = "CSV", uris = ["%s"],max_bad_records = 2, skip_leading_rows=1)';

BEGIN

    -- Example of a static external table creation (commented out)
    -- CREATE OR REPLACE EXTERNAL TABLE rawds.cust_ext ( custno INT64,firstname STRING,lastname STRING,age INT64,profession STRING) OPTIONS (   format = "CSV", uris = ["gs://data-samples/dataset/bqdata/ext_src_data/custs_header_20230908"],max_bad_records = 2, skip_leading_rows=1);

    -- Execute the dynamically built SQL query to create the BigLake external table.
    EXECUTE IMMEDIATE dynamicsql;
    -- EXECUTE IMMEDIATE FORMAT(dynamicsql, v_uri); -- If using FORMAT for dynamic SQL
    -- Step 1: BigLake external table creation is completed

    -- Step 2: SCD Type 2 Implementation (maintain history for changes)
    -- This table will store historical versions of customer data, partitioned by date.
    CREATE TABLE IF NOT EXISTS curatedds.cust_part_curated_scd2_append (
        custno INT64,
        name STRING,
        age INT64,
        profession STRING,
        datadt DATE,
        upd_ts TIMESTAMP
    )
    PARTITION BY datadt;

    -- To prevent accidentally loading the same day's data more than once,
    -- delete any existing records for the current `v_datadt`.
    -- Previous days' data will remain untouched, ensuring history is preserved.
    DELETE FROM curatedds.cust_part_curated_scd2_append WHERE datadt = v_datadt;

    -- Insert new or updated records from the external table into the SCD2 table.
    -- CONCAT is used to combine first and last names into a single 'name' field.
    -- COALESCE handles potential NULL values for 'profession' by replacing them with 'na'.
    INSERT INTO curatedds.cust_part_curated_scd2_append
    SELECT
        custno,
        CONCAT(firstname, ',', lastname) AS name,
        age,
        COALESCE(profession, 'na') AS profession,
        v_datadt,
        upd_ts
    FROM rawds.cust_ext;

    -- Display a count of records per date in the SCD2 table (for verification).
    SELECT
        datadt,
        COUNT(1)
    FROM curatedds.cust_part_curated_scd2_append
    GROUP BY
        DATADT
    LIMIT 10;

    -- Step 2: Loading SCD2 table is completed

    -- SCD Type 1 Implementation (overwrite history, only keep current state)
    -- This table will store only the most recent version of customer data, partitioned by date.
    CREATE TABLE IF NOT EXISTS curatedds.cust_part_curated_scd1_merge (
        custno INT64,
        name STRING,
        age INT64,
        profession STRING,
        datadt DATE,
        upd_ts TIMESTAMP
    )
    PARTITION BY datadt;

    -- MERGE is a powerful DML statement that can perform inserts, updates, and deletes
    -- in a single query, based on matching conditions.
    -- Here, it updates existing records (MATCHED) and inserts new ones (NOT MATCHED).
    MERGE `curatedds.cust_part_curated_scd1_merge` T
    USING (
        SELECT
            custno,
            CONCAT(firstname, ',', lastname) AS name,
            age,
            COALESCE(profession, 'na') AS profession,
            CAST(v_datadt AS DATE) AS datadt,
            upd_ts
        FROM rawds.cust_ext
    ) S
    ON T.custno = S.custno
    WHEN MATCHED THEN
        UPDATE SET
            T.name = S.name,
            T.age = S.age,
            T.profession = S.profession,
            T.datadt = S.datadt,
            T.upd_ts = S.upd_ts
    WHEN NOT MATCHED THEN
        INSERT (custno, name, age, profession, datadt, upd_ts)
        VALUES (S.custno, S.name, S.age, S.profession, S.datadt, S.upd_ts);

    -- Display a count of records per date in the SCD1 table (for verification).
    SELECT
        datadt,
        COUNT(1)
    FROM curatedds.cust_part_curated_scd1_merge
    GROUP BY
        DATADT
    LIMIT 10;

END;

/*
Illustrative Example of SCD1 vs SCD2:

Source Data (src):
Day 1:
1,chn
2,mum

Day 2 (delta/CDC data):
1,blr (customer 1's city changed from 'chn' to 'blr')
3,hyd (new customer 3)

Load 1: Don't maintain history (SCD Type 1 - using MERGE)
+ Benefits: No duplicates, saves storage space.
- Drawbacks: Cannot see historical changes.
Target after Day 1 + Day 2 load:
custno, name, datadt
1,blr,Day2 (Updated)
2,mum,Day1 (Unchanged)
3,hyd,Day2 (Inserted)

Load 2: Maintain history (SCD Type 2 - using DELETE + INSERT)
+ Benefits: Can see the full history of changes.
- Drawbacks: Leads to more duplicates and larger storage.
Target after Day 1 load:
custno, name, datadt
1,chn,Jan1
2,mum,Jan1

Target after Day 2 load (new rows added for changes):
custno, name, datadt
1,chn,Jan1
2,mum,Jan1
1,blr,Jul26 (Inserted new record for change in customer 1)
3,hyd,Jul26 (Inserted new record for customer 3)
*/
