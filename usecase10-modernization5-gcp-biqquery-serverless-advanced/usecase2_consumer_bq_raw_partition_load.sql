-- Author: muralitheda
-- Project: Data Curation and Change Data Capture (CDC)
-- Purpose: This script demonstrates how to build a BigQuery ETL pipeline
--          that leverages external tables for data ingestion and implements
--          both Slowly Changing Dimension Type 1 (SCD1) and Type 2 (SCD2)
--          strategies using BigQuery's procedural language and MERGE statements.
--          It processes data from a specified external URI, curates it,
--          and loads it into partitioned BigQuery tables.

-- Declare variables for URI, data date, raw data date string, and dynamic SQL.
-- These variables are session-scoped and allow for flexible and parameterized execution of the script.
DECLARE v_uri STRING;          -- Stores the Google Cloud Storage URI of the source data file.
DECLARE v_datadt DATE;         -- Stores the parsed date extracted from the URI, used for partitioning and CDC.
DECLARE v_datadtraw STRING;    -- Stores the raw date string extracted from the URI.
DECLARE dynamicsql STRING;     -- Stores the dynamically constructed SQL query for creating the external table.

-- Set the URI for the external data source using a parameter.
-- The parameter @param1 is a placeholder that will be supplied when this script
-- is executed, typically from an orchestration tool or a BigQuery stored procedure call.
SET v_uri = @param1;

-- Extract the raw date string from the end of the URI (last 8 characters).
-- This assumes the URI always ends with an 8-digit date in YYYYMMDD format.
-- Example: 'gs://bucket/path/to/file_20230908' would extract '20230908'.
SET v_datadtraw = (SELECT RIGHT(v_uri, 8));

-- Parse the raw date string (YYYYMMDD) into a DATE data type.
-- This converted date 'v_datadt' will be used for filtering, partitioning,
-- and as the effective date for CDC operations.
SET v_datadt = (SELECT PARSE_DATE('%Y%m%d', v_datadtraw));

-- CDC (Change Data Capture) based on the date parameter suffixed in the filename.
-- This dynamic SQL string constructs a 'CREATE OR REPLACE EXTERNAL TABLE' statement.
-- External tables allow querying data directly from Google Cloud Storage without loading it
-- into BigQuery's managed storage, providing flexibility for raw data access.
-- 'CONCAT' function is used to combine static SQL parts with the dynamic 'v_uri' variable.
SET dynamicsql = CONCAT(
    'CREATE OR REPLACE EXTERNAL TABLE rawds.cust_ext ( ',
    'custno INT64,firstname STRING,lastname STRING,age INT64,profession STRING,upd_ts timestamp',
    ') OPTIONS ( ',
    'format = "CSV", ',                 -- Specifies the format of the external data as CSV.
    'uris = ["', v_uri, '"],',          -- Points to the specific GCS URI containing the data.
    'max_bad_records = 2, ',            -- Allows up to 2 malformed records before the job fails.
    'skip_leading_rows=1',              -- Skips the first row of the CSV file, assuming it's a header.
    ')'
);

-- Begin a procedural block. All statements within this block are executed sequentially.
-- This provides a structured way to manage multiple DDL and DML operations.
BEGIN

    -- Execute the dynamically built SQL query string.
    -- 'EXECUTE IMMEDIATE' is crucial for running SQL statements that are constructed as strings,
    -- allowing for dynamic table names, column names, or other SQL elements.
    -- This creates or replaces the 'rawds.cust_ext' external table,
    -- which acts as a temporary view over the incoming raw CSV data.
    EXECUTE IMMEDIATE dynamicsql;

    -- SCD2 (Slowly Changing Dimension Type 2) Implementation:
    -- This approach is used when it's necessary to maintain the full history of changes for each record.
    -- For example, if a customer's profession changes, a new record is inserted with the new profession
    -- and an effective date, while the old record is marked as inactive or given an end date (though
    -- this script primarily focuses on inserting new daily snapshots).
    -- Create the 'curatedds.cust_part_curated' table if it does not exist.
    -- This table is partitioned by 'datadt' to optimize queries by date, reducing scan costs and improving performance.
    CREATE TABLE IF NOT EXISTS curatedds.cust_part_curated (
        custno INT64,         -- Customer number
        name STRING,          -- Concatenated full name of the customer
        age INT64,            -- Age of the customer
        profession STRING,    -- Profession of the customer
        datadt DATE,          -- The effective date of the data, derived from the input URI
        upd_ts TIMESTAMP      -- Timestamp of the update/load
    )
    PARTITION BY datadt;      -- Partitions the table by the 'datadt' column.

    -- Delete existing data for the current processing date (v_datadt) from the SCD2 table.
    -- This step is critical for ensuring idempotency for the current day's load.
    -- If the job is rerun for the same 'v_datadt', this DELETE statement ensures that
    -- any previously loaded data for that specific date is removed before new data is inserted,
    -- preventing duplicate records for the same effective date.
    DELETE FROM curatedds.cust_part_curated WHERE datadt = v_datadt;

    -- Insert new and updated data from the external table ('rawds.cust_ext') into the SCD2 table.
    -- Data transformations applied during insertion:
    -- 1. `CONCAT(firstname, ",", lastname)`: Combines first and last names into a single 'name' field.
    -- 2. `COALESCE(profession, 'na')`: Replaces any NULL values in the 'profession' column with 'na' (not applicable).
    -- 3. `v_datadt`: The date derived from the URI is used as the 'datadt' for partitioning.
    INSERT INTO curatedds.cust_part_curated
    SELECT
        custno,
        CONCAT(firstname, ",", lastname) AS name,
        age,
        COALESCE(profession, 'na') AS profession,
        v_datadt, -- The date derived from the URI, representing the data's effective date
        upd_ts
    FROM rawds.cust_ext;

    -- Select and count data by 'datadt' from the SCD2 table to verify the load.
    -- This provides a quick check of the data loaded for the current processing date.
    SELECT datadt, COUNT(1) FROM curatedds.cust_part_curated GROUP BY DATADT LIMIT 10;

    -- SCD1 (Slowly Changing Dimension Type 1) Implementation:
    -- This approach overwrites existing data for a given key, meaning historical changes are NOT maintained.
    -- Only the most recent state of a record is preserved.
    -- Create the 'curatedds.cust_part_curated_merge' table if it does not exist.
    -- This table is also partitioned by 'datadt'.
    CREATE TABLE IF NOT EXISTS curatedds.cust_part_curated_merge (
        custno INT64,         -- Customer number (primary key for merge)
        name STRING,          -- Concatenated full name
        age INT64,            -- Age
        profession STRING,    -- Profession
        datadt DATE,          -- Effective date of the record
        upd_ts TIMESTAMP      -- Timestamp of the update
    )
    PARTITION BY datadt;      -- Partitions the table by the 'datadt' column.

    -- Use a MERGE statement for SCD1 implementation.
    -- MERGE is a powerful DML statement that performs UPSERT (UPDATE or INSERT) operations
    -- based on whether a row from the source (S) matches a row in the target (T).
    MERGE `curatedds.cust_part_curated_merge` T
    USING (
        -- Source data for the MERGE operation, derived from the external table.
        SELECT
            custno,
            CONCAT(firstname, ",", lastname) AS name,
            age,
            COALESCE(profession, 'na') AS profession,
            CAST(v_datadt AS DATE) AS datadt, -- Ensure 'v_datadt' is explicitly cast to DATE
            upd_ts
        FROM rawds.cust_ext
    ) S
    ON T.custno = S.custno -- Join condition: records are matched based on 'custno'.
    WHEN MATCHED THEN
        -- If a record with the same 'custno' exists in the target table (T), update its attributes
        -- with the latest values from the source (S). This overwrites old values, hence SCD1.
        UPDATE SET
            T.name = S.name,
            T.age = S.age,
            T.profession = S.profession,
            T.datadt = S.datadt,
            T.upd_ts = S.upd_ts
    WHEN NOT MATCHED THEN
        -- If no matching record is found in the target table (T), insert the new record from the source (S).
        INSERT (custno, name, age, profession, datadt, upd_ts)
        VALUES (S.custno, S.name, S.age, S.profession, S.datadt, S.upd_ts);

    -- Select and count data by 'datadt' from the SCD1 table to verify the load.
    -- This provides a quick check of the data loaded for the current processing date.
    SELECT datadt, COUNT(1) FROM curatedds.cust_part_curated_merge GROUP BY DATADT LIMIT 10;

END;
