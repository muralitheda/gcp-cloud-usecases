-- Author: muralitheda
-- Project: Data Curation and Change Data Capture (CDC) Pipeline
-- Purpose: This stored procedure automates the ETL process for customer data,
--          demonstrating the use of BigQuery's procedural language, dynamic SQL,
--          external tables, and implementation of Slowly Changing Dimension (SCD)
--          strategies (Type 1 and Type 2) with comprehensive audit logging and
--          error handling. It processes data from a specified Google Cloud Storage URI
--          and loads it into partitioned BigQuery curated tables.

-- Procedure Definition:
-- `CREATE OR REPLACE PROCEDURE` allows updating the procedure if it already exists.
-- `weekday-32-proj-427802-a6.curatedds.sp_cust_etl` specifies the full path to the procedure
-- (project.dataset.procedure_name).
-- Parameters:
--   `p_uri STRING`: Input parameter for the Google Cloud Storage URI of the source CSV file.
--   `flag STRING`: Input parameter to determine which SCD strategy ('scd1' or 'scd2') to apply.
CREATE OR REPLACE PROCEDURE `iz-cloud-training-project.curatedds.sp_cust_etl`(p_uri STRING, flag STRING)
BEGIN
    -- Variable Declarations:
    -- These variables are declared at the beginning of the `BEGIN...END` block,
    -- making them accessible throughout the procedure's execution.
    DECLARE v_uri STRING;          -- Stores the Google Cloud Storage URI passed as 'p_uri'.
    DECLARE v_datadt DATE;         -- Stores the parsed date extracted from the URI, used for partitioning and CDC.
    DECLARE v_datadtraw STRING;    -- Stores the raw date string extracted from the URI.
    DECLARE dynamicsql STRING;     -- Stores the dynamically constructed SQL query for creating the external table.

    -- Variable Assignments:
    -- Assign the input parameter 'p_uri' to the internal variable 'v_uri'.
    SET v_uri = p_uri;

    -- Extract the raw date string from the end of the URI (last 8 characters).
    -- This assumes the URI always ends with an 8-digit date in YYYYMMDD format, e.g., '.../custs_header_20230908'.
    SET v_datadtraw = (SELECT RIGHT(v_uri, 8));

    -- Parse the raw date string (YYYYMMDD) into a DATE data type.
    -- This converted date 'v_datadt' will be a key identifier for the data's effective date,
    -- used for partitioning and for idempotency in CDC operations.
    SET v_datadt = (SELECT PARSE_DATE('%Y%m%d', v_datadtraw));

    -- Dynamic SQL Construction for External Table:
    -- This section constructs a `CREATE OR REPLACE EXTERNAL TABLE` statement dynamically.
    -- External tables in BigQuery allow querying data directly from Google Cloud Storage
    -- without requiring the data to be loaded into BigQuery's native storage.
    -- This is particularly useful for raw data access, ad-hoc queries, or when data
    -- frequently changes in the source GCS bucket.
    SET dynamicsql = CONCAT(
        'CREATE OR REPLACE EXTERNAL TABLE rawds.cust_ext ( ',
        'custno INT64,firstname STRING,lastname STRING,age INT64,profession STRING,upd_ts timestamp',
        ') OPTIONS ( ',
        'format = "CSV", ',                 -- Specifies the format of the external data as CSV.
        'uris = ["', v_uri, '"],',          -- Points to the specific GCS URI containing the data.
        'max_bad_records = 2, ',            -- Allows up to 2 malformed records in the CSV before the job fails.
        'skip_leading_rows=1',              -- Skips the first row of the CSV file, assuming it's a header.
        ')'
    );

    -- Begin a nested procedural block for External Table creation and its audit logging.
    -- This block includes `EXCEPTION WHEN ERROR` for specific error handling.
    BEGIN
        -- Create an audit table if it doesn't exist.
        -- This table is used to log the status (START, SUCCESS, FAILURE) of different process steps,
        -- including any error messages.
        CREATE TABLE IF NOT EXISTS curatedds.audit_tbl (
            processid STRING,    -- Unique ID for the process instance (e.g., UUID)
            jobname STRING,      -- Name of the job/procedure
            processtype STRING,  -- Description of the specific step being audited
            errormsg STRING,     -- Error message if an exception occurs
            errtype STRING,      -- Type of audit entry (START, SUCCESS, FAILURE)
            TS TIMESTAMP         -- Timestamp of the audit entry
        );

        -- Log the start of the external table creation process.
        INSERT INTO `curatedds.audit_tbl` (processid, jobname, processtype, errormsg, errtype, ts)
        VALUES (GENERATE_UUID(), 'sp_cust_etl', '1.Creating external table cust_ext referring the gcs parameter location', '', 'START', CURRENT_TIMESTAMP());

        -- Execute the dynamically constructed SQL query to create/replace the external table.
        -- This effectively makes the GCS data accessible as 'rawds.cust_ext' within BigQuery.
        EXECUTE IMMEDIATE dynamicsql;

        -- Log the successful completion of the external table creation.
        INSERT INTO `curatedds.audit_tbl` (processid, jobname, processtype, errormsg, errtype, ts)
        VALUES (GENERATE_UUID(), 'sp_cust_etl', '1.Created external table cust_ext referring the gcs parameter location', '', 'SUCCESS', CURRENT_TIMESTAMP());

    EXCEPTION WHEN ERROR THEN
        -- Error Handling for External Table Creation:
        -- If an error occurs during the `EXECUTE IMMEDIATE dynamicsql` statement,
        -- this block catches the error and logs the failure details into the audit table.
        -- `@@error.message` provides the specific error message from BigQuery.
        INSERT INTO `curatedds.audit_tbl` (processid, jobname, processtype, errormsg, errtype, ts)
        VALUES (GENERATE_UUID(), 'sp_cust_etl', '1. Created external table cust_ext referring the gcs parameter location Failed', @@error.message, 'FAILURE', CURRENT_TIMESTAMP());

    END; -- End of nested block for External Table creation

    -- Conditional Logic based on 'flag' parameter:
    -- This `IF...ELSEIF...END IF` structure allows the stored procedure to execute
    -- different CDC strategies (SCD1 or SCD2) or a default action based on the input 'flag'.
    IF flag = 'scd2' THEN
        -- SCD2 (Slowly Changing Dimension Type 2) Implementation Block:
        -- This block handles the loading of customer data into a partitioned table
        -- while maintaining the full history of changes for each customer.
        BEGIN
            -- Create the 'curatedds.cust_part_curated' table if it does not exist.
            -- This table stores customer data with a partition on 'datadt' for performance.
            CREATE TABLE IF NOT EXISTS curatedds.cust_part_curated (
                custno INT64,
                name STRING,
                age INT64,
                profession STRING,
                datadt DATE,
                upd_ts TIMESTAMP
            )
            PARTITION BY datadt;

            -- Idempotent Delete for SCD2:
            -- Delete existing data for the current processing date ('v_datadt') from the SCD2 table.
            -- This is crucial for idempotency: if the procedure is rerun for the same day's data,
            -- this ensures that any previously loaded data for that specific 'datadt' is removed
            -- before new data is inserted, preventing duplicate records for the same effective date.
            DELETE FROM curatedds.cust_part_curated WHERE datadt = v_datadt;

            -- Log the start of the SCD2 data load process.
            INSERT INTO `curatedds.audit_tbl` (processid, jobname, processtype, errormsg, errtype, ts)
            VALUES (GENERATE_UUID(), 'sp_cust_etl', '2.curated table cust_part_curated load', '', 'START', CURRENT_TIMESTAMP());

            -- Insert new and updated customer data from the external table ('rawds.cust_ext')
            -- into the SCD2 partitioned table.
            -- Transformations: concatenates first/last name, handles NULL professions.
            INSERT INTO curatedds.cust_part_curated
            SELECT
                custno,
                CONCAT(firstname, ",", lastname) AS name,
                age,
                COALESCE(profession, 'na') AS profession,
                v_datadt, -- The effective date for this snapshot of data
                upd_ts
            FROM rawds.cust_ext;

            -- Log the successful completion of the SCD2 data load.
            INSERT INTO `curatedds.audit_tbl` (processid, jobname, processtype, errormsg, errtype, ts)
            VALUES (GENERATE_UUID(), 'sp_cust_etl', '2.curated table cust_part_curated load', '', 'SUCCESS', CURRENT_TIMESTAMP());

            -- Verify the load by selecting and counting data for the current 'datadt'.
            SELECT datadt, COUNT(1) FROM curatedds.cust_part_curated GROUP BY DATADT LIMIT 10;

        EXCEPTION WHEN ERROR THEN
            -- Error Handling for SCD2 Load:
            -- If an error occurs during the SCD2 load, log the failure details.
            INSERT INTO `curatedds.audit_tbl` (processid, jobname, processtype, errormsg, errtype, ts)
            VALUES (GENERATE_UUID(), 'sp_cust_etl', '2.curated table cust_part_curated load Failed', @@error.message, 'FAILURE', CURRENT_TIMESTAMP());

        END; -- End of SCD2 block

    ELSEIF flag = 'scd1' THEN
        -- SCD1 (Slowly Changing Dimension Type 1) Implementation Block:
        -- This block handles the loading of customer data where only the most recent state
        -- of a record is maintained. Historical changes are overwritten.
        BEGIN
            -- Create the 'curatedds.cust_part_curated_merge' table if it does not exist.
            -- This table also stores customer data, partitioned by 'datadt'.
            CREATE TABLE IF NOT EXISTS curatedds.cust_part_curated_merge (
                custno INT64,
                name STRING,
                age INT64,
                profession STRING,
                datadt DATE,
                upd_ts TIMESTAMP
            )
            PARTITION BY datadt;

            -- Log the start of the SCD1 MERGE load process.
            INSERT INTO `curatedds.audit_tbl` (processid, jobname, processtype, errormsg, errtype, ts)
            VALUES (GENERATE_UUID(), 'sp_cust_etl', '3.curated table cust_part_curated_merged load', '', 'START', CURRENT_TIMESTAMP());

            -- MERGE statement for SCD1 implementation (UPSERT logic):
            -- This statement efficiently updates existing records or inserts new ones.
            -- It compares records from the source (external table `rawds.cust_ext`) with the target table.
            MERGE `curatedds.cust_part_curated_merge` T  -- Target table
            USING (
                -- Source data (S) for the MERGE operation, derived from the external table.
                -- Includes transformations like name concatenation and null handling.
                SELECT
                    custno,
                    CONCAT(firstname, ",", lastname) AS name,
                    age,
                    COALESCE(profession, 'na') AS profession,
                    CAST(v_datadt AS DATE) AS datadt, -- Ensure 'v_datadt' is explicitly cast to DATE
                    upd_ts
                FROM rawds.cust_ext
            ) S -- Source data alias
            ON T.custno = S.custno -- Join condition: Records are matched on 'custno'.
            WHEN MATCHED THEN
                -- If a record with the same 'custno' is found in the target table (T),
                -- update its attributes with the latest values from the source (S).
                -- This overwrites old values, hence it's an SCD1 behavior.
                UPDATE SET
                    T.name = S.name,
                    T.age = S.age,
                    T.profession = S.profession,
                    T.datadt = S.datadt,
                    T.upd_ts = S.upd_ts
            WHEN NOT MATCHED THEN
                -- If no matching record is found in the target table (T),
                -- insert the new record from the source (S).
                INSERT (custno, name, age, profession, datadt, upd_ts)
                VALUES (S.custno, S.name, S.age, S.profession, S.datadt, S.upd_ts);

            -- Verify the load by selecting and counting data for the current 'datadt'.
            SELECT datadt, COUNT(1) FROM curatedds.cust_part_curated_merge GROUP BY DATADT LIMIT 10;

            -- Log the successful completion of the SCD1 MERGE load.
            INSERT INTO `curatedds.audit_tbl` (processid, jobname, processtype, errormsg, errtype, ts)
            VALUES (GENERATE_UUID(), 'sp_cust_etl', '3.curated table cust_part_curated_merged load', '', 'SUCCESS', CURRENT_TIMESTAMP());

        EXCEPTION WHEN ERROR THEN
            -- Error Handling for SCD1 Load:
            -- If an error occurs during the SCD1 MERGE load, log the failure details.
            INSERT INTO `curatedds.audit_tbl` (processid, jobname, processtype, errormsg, errtype, ts)
            VALUES (GENERATE_UUID(), 'sp_cust_etl', '3.curated table cust_part_curated_merged load Failed', @@error.message, 'FAILURE', CURRENT_TIMESTAMP());

        END; -- End of SCD1 block

    ELSE
        -- Default action if an unexpected 'flag' argument is passed.
        -- This ensures the procedure handles invalid inputs gracefully.
        DROP TABLE IF EXISTS rawds.cust_ext; -- Clean up the external table if no valid flag is provided.
        INSERT INTO `curatedds.audit_tbl` (processid, jobname, processtype, errormsg, errtype, ts)
        VALUES (GENERATE_UUID(), 'sp_cust_etl', '0. Nothing happened due to unexpected argument passed, expected is scd1 or scd2', '', 'SUCCESS', CURRENT_TIMESTAMP());
    END IF;

END; -- End of main procedure block
