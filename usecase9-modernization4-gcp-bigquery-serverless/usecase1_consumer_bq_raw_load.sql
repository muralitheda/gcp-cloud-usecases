/*
author : muralitheda
*/

--drop table rawds.trans_online;
--drop table rawds.consumer;
--drop table rawds.trans_pos;
--drop table rawds.trans_mobile_channel;
--drop table curatedds.consumer_full_load;
--drop table curatedds.trans_online_part;
--drop table curatedds.trans_pos_part_cluster;
--drop table `curatedds.trans_mobile_autopart_2021`;
--drop table `curatedds.trans_mobile_autopart_2022`;
--drop table `curatedds.trans_mobile_autopart_2023`;

-- Create schema if it does not exist, with location 'us'
CREATE SCHEMA IF NOT EXISTS rawds
OPTIONS(location='us');

-- Create audit_meta table if it does not exist
CREATE TABLE IF NOT EXISTS rawds.audit_meta (
    proc_name   STRING,
    exec_ts     TIMESTAMP,
    description STRING
);

-- Log: Load process started
SELECT CURRENT_TIMESTAMP, "Load started";

------------------------------------------------
-- Log: Step 1- Consumer Dimension data load
------------------------------------------------
INSERT INTO rawds.audit_meta
SELECT
    'rawload',
    CURRENT_TIMESTAMP,
    "[INFO] 1. Consumer Dimension - Create manually the table and Load CSV data into BQ Managed table, skip the header column in the file";

SELECT CURRENT_TIMESTAMP, "[INFO] 1. Consumer Dimension - Create manually the table and Load CSV data into BQ Managed table, skip the header column in the file";

-- Create consumer table with predefined schema
CREATE TABLE IF NOT EXISTS rawds.consumer (
    custno     INT64,
    firstname  STRING,
    lastname   STRING,
    age        INT64,
    profession STRING
);

-- Load CSV data into rawds.consumer, skipping header row
LOAD DATA OVERWRITE rawds.consumer
FROM FILES (
    format = 'CSV',
    uris = ['gs://iz-cloud-training-project-bucket/datasets/custs_header'],
    skip_leading_rows = 1,
    field_delimiter = ','
);

------------------------------------------------
-- Log: Step 2 - Store POS data load
------------------------------------------------
INSERT INTO rawds.audit_meta
SELECT
    'rawload',
    CURRENT_TIMESTAMP,
    "[INFO] 2. Store POS - Create and Load CSV data into BQ Managed table with defined schema";

SELECT CURRENT_TIMESTAMP, "[INFO] 2. Store POS - Create and Load CSV data into BQ Managed table with defined schema";

-- Complete Load (Delete/Truncate and Load) for rawds.trans_pos
LOAD DATA OVERWRITE `rawds.trans_pos` (
    txnno     NUMERIC,
    txndt     STRING,
    custno    INT64,
    amt       FLOAT64,
    category  STRING,
    product   STRING,
    city      STRING,
    state     STRING,
    spendby   STRING
)
FROM FILES (
    format = 'CSV',
    uris = ['gs://iz-cloud-training-project-bucket/datasets/store_pos_product_trans.csv'],
    field_delimiter = ','
);

------------------------------------------------
-- Log: Step 3 - Online Transactions data load
------------------------------------------------
INSERT INTO rawds.audit_meta
SELECT
    'rawload',
    CURRENT_TIMESTAMP,
    "[INFO] 3. Online Trans - Create and Load JSON data into BQ Managed table using auto detect schema";

SELECT CURRENT_TIMESTAMP, "[INFO] 3. Online Trans - Create and Load JSON data into BQ Managed table using auto detect schema";

-- Load JSON data into rawds.trans_online with auto-detect schema
LOAD DATA OVERWRITE rawds.trans_online
FROM FILES (
    format = 'JSON',
    uris = ['gs://iz-cloud-training-project-bucket/datasets/online_products_trans.json']
);

------------------------------------------------
-- Log: Step 4 - Mobile Transactions data load
------------------------------------------------
INSERT INTO rawds.audit_meta
SELECT
    'rawload',
    CURRENT_TIMESTAMP,
    "[INFO] 4. Mobile Trans - Create the table using auto detect schema using the header column and Load CSV data into BQ Managed table";

SELECT CURRENT_TIMESTAMP, "[INFO] 4. Mobile Trans - Create the table using auto detect schema using the header column and Load CSV data into BQ Managed table";

-- Load CSV data into rawds.trans_mobile_channel with auto-detect schema
LOAD DATA OVERWRITE `rawds.trans_mobile_channel`
FROM FILES (
    format = 'CSV',
    uris = ['gs://iz-cloud-training-project-bucket/datasets/mobile_trans.csv'],
    field_delimiter = ','
);

-- Log: Load process completed successfully
INSERT INTO rawds.audit_meta
SELECT
    'rawload',
    CURRENT_TIMESTAMP,
    "[INFO] 5. Load completed Successfully";
