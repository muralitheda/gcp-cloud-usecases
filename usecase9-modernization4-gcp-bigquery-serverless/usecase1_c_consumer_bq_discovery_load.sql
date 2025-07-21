/*
Author: muralitheda
Project: Data Curation and Discovery Layer
Purpose: This script performs data wrangling, denormalization, and aggregation
         to create discovery-ready tables in BigQuery.
*/

-- Create discoveryds schema if it does not exist, with location 'us'
-- This schema will store the curated and aggregated data for analytical purposes.
CREATE SCHEMA IF NOT EXISTS discoveryds
OPTIONS(location='us');

-- Create discoveryds.consumer_trans_pos_mob_online table
-- This table is designed to be a wide, denormalized fact table.
-- It combines consumer details with transactions from Point-of-Sale (POS), Mobile, and Online channels.
CREATE TABLE IF NOT EXISTS discoveryds.consumer_trans_pos_mob_online (
    txnno               NUMERIC,
    txndt               DATE,
    custno              INT64,
    fullname            STRING,
    age                 INT64,
    profession          STRING,
    trans_day           STRING,      -- Indicates if transaction occurred on the same day across channels
    trans_type          STRING,      -- Type of transaction (POS, MOB, ONLINE, POS_MOB)
    net                 STRING,      -- Mobile network (if applicable)
    online_pos_amount   FLOAT64,     -- Consolidated transaction amount from online/POS
    geo_coordinate      GEOGRAPHY,   -- Geographical coordinates (if from mobile)
    provider            STRING,      -- Mobile service provider
    activity            STRING,      -- Mobile activity (e.g., 'In Store Pickup')
    spendby             STRING,      -- Who made the purchase (e.g., 'Card', 'Cash')
    city                STRING,
    state               STRING,
    online_pos_category STRING,      -- Consolidated product category from online/POS
    product             STRING,      -- Product name (if from POS)
    loadts              TIMESTAMP,   -- Load timestamp
    loaddt              DATE         -- Load date
);

-- Data Wrangling: Joining, widening, and denormalizing data using Common Table Expressions (CTEs)
-- CTEs (defined with the `WITH` clause) are used here to improve readability and reusability of complex queries.
-- They are processed once and their result is typically materialized (cached) during query execution,
-- which can be beneficial for breaking down large queries and managing intermediate data, especially for smaller to medium data volumes.
-- This section focuses on:
-- 1. Enriching data by joining different sources.
-- 2. Ensuring data coverage by handling nulls and providing default values.
-- 3. Stitching together consumer events across multiple channels (POS, Mobile, Online) into a single, wide record.
-- The joins standardize and create a wide, denormalized table for easier downstream analysis.

INSERT INTO discoveryds.consumer_trans_pos_mob_online

WITH
-- CTE 1: `pos_mob_trans`
-- Purpose: Joins Mobile and Point-of-Sale (POS) transactions.
-- It identifies transactions that occurred on the same day or across multiple days,
-- and determines the transaction type (Mobile, POS, or both).
pos_mob_trans AS (
    SELECT
        mob.txnno AS mob_txnno,
        pos.txnno AS pos_txnno,
        CASE
            WHEN mob.dt = pos.txndt THEN 'Same Day Trans' -- Indicates if mobile and POS transactions occurred on the same date
            ELSE 'Multi Day Trans'
        END AS trans_day,
        COALESCE(mob.dt, pos.txndt) AS txndt, -- Selects the transaction date, preferring mobile's date if available
        CASE
            WHEN mob.txnno IS NULL THEN 'POS'      -- If only a POS transaction exists
            WHEN pos.txnno IS NULL THEN 'MOB'      -- If only a Mobile transaction exists
            ELSE 'POS_MOB'                         -- If both POS and Mobile transactions exist for the same txnno
        END AS trans_type,
        mob.net,
        pos.amount,
        mob.geo_coordinate,
        mob.provider,
        CASE
            WHEN mob.activity = 'STILL' THEN 'In Store Pickup' -- Standardizes 'STILL' activity to 'In Store Pickup' for consistency
            ELSE mob.activity
        END AS activity,
        pos.spendby,
        pos.city,
        pos.state,
        pos.product,
        pos.category,
        pos.loadts AS pos_loadts,
        pos.loaddt AS pos_loaddt,
        mob.loadts AS mob_loadts,
        mob.loaddt AS mob_loaddt,
        pos.custno
    FROM
        `curatedds.trans_mobile_autopart_20*` AS mob -- Wildcard table query for mobile transactions across all years (e.g., 2021, 2022, 2023)
    FULL JOIN
        `curatedds.trans_pos_part_cluster` AS pos    -- Joins with the clustered POS transactions table
    ON
        mob.txnno = pos.txnno                      -- Joins on transaction number to link related transactions
),

-- CTE 2: `cust_online_trans`
-- Purpose: Joins the Customer dimension table with Online transactions.
-- This enriches online transaction data with core customer details and marks the transaction type as 'online'.
-- Ambiguity Resolution: Explicitly select columns and alias `loadts` and `loaddt` from `cust` and `trans`
-- to avoid naming conflicts when both tables contain these columns.
cust_online_trans AS (
    SELECT
        cust.custno,
        cust.fullname,
        cust.age,
        cust.yearofbirth,   -- Explicitly select yearofbirth from customer
        cust.profession,
        cust.loadts AS consumer_loadts, -- Aliased to avoid ambiguity with online transaction loadts
        cust.loaddt AS consumer_loaddt, -- Aliased to avoid ambiguity with online transaction loaddt
        trans.transsk,
        trans.productname,
        trans.productcategory,
        trans.productprice,
        trans.productidint,
        trans.prodidstr,
        trans.loadts AS online_trans_loadts, -- Aliased to distinguish from consumer_loadts
        trans.loaddt AS online_trans_loaddt, -- Aliased to distinguish from consumer_loaddt
        CASE
            WHEN trans.transsk IS NOT NULL THEN 'online' -- Assigns 'online' as transaction type if an online transaction ID exists
            ELSE NULL
        END AS online_trans_type -- Aliased to distinguish from `trans_type` in `pos_mob_trans` CTE
    FROM
        `curatedds.consumer_full_load` AS cust -- Customer master data (fully loaded and curated)
    LEFT JOIN
        `curatedds.trans_online_part` AS trans -- Online transactions (partitioned table)
    ON
        cust.custno = trans.customerid -- Joins on customer number to link customers to their online activities
)

-- Final SELECT statement to combine all transaction types and customer data
-- This query performs the final denormalization and consolidation into the `consumer_trans_pos_mob_online` table.
-- It intelligently handles potential `NULL` values resulting from `JOIN` operations, ensuring comprehensive records
-- by coalescing values from different sources.
SELECT
    COALESCE(trans.mob_txnno, trans.pos_txnno, cust.transsk) AS txnno, -- Consolidates transaction number from mobile, POS, or online
    -- For txndt, prioritize transaction date from POS/Mobile, then use online transaction's load date as a fallback if no specific transaction date is available for online.
    COALESCE(trans.txndt, cust.online_trans_loaddt) AS txndt,
    cust.custno,
    cust.fullname,
    cust.age,
    cust.profession,
    trans.trans_day,
    COALESCE(cust.online_trans_type, trans.trans_type) AS trans_type, -- Consolidates the transaction type (e.g., 'online', 'POS', 'MOB', 'POS_MOB')
    COALESCE(trans.net, 'na') AS net,                                 -- Defaults mobile network to 'na' if not available
    COALESCE(cust.productprice, COALESCE(trans.amount, 0.0)) AS online_pos_amount, -- Prioritizes online product price, then POS/mobile amount, defaulting to 0.0
    trans.geo_coordinate,
    COALESCE(trans.provider, 'na') AS provider,                       -- Defaults mobile provider to 'na' if not available
    COALESCE(trans.activity, 'na') AS activity,                       -- Defaults mobile activity to 'na' if not available
    COALESCE(trans.spendby, 'na') AS spendby,                         -- Defaults POS spendby to 'na' if not available
    COALESCE(trans.city, 'unknown') AS city,                          -- Defaults city to 'unknown' if not available
    COALESCE(trans.state, 'unknown') AS state,                        -- Defaults state to 'unknown' if not available
    COALESCE(cust.productcategory, trans.category) AS online_pos_category, -- Consolidates product category from online or POS
    trans.product,                                                    -- Product name, primarily from POS
    COALESCE(trans.pos_loadts, trans.mob_loadts, cust.consumer_loadts) AS loadts, -- Consolidates load timestamp from available sources
    COALESCE(trans.pos_loaddt, trans.mob_loaddt, cust.consumer_loaddt) AS loaddt  -- Consolidates load date from available sources
FROM
    cust_online_trans AS cust -- Left join with the combined customer and online transactions
LEFT OUTER JOIN
    pos_mob_trans AS trans    -- Outer join with the combined POS and mobile transactions
ON
    trans.custno = cust.custno; -- Links the two CTEs on customer number

-- Create discoveryds.trans_aggr table for aggregated transaction data
-- This table stores summarized transaction metrics, useful for reporting and dashboards.
CREATE TABLE IF NOT EXISTS discoveryds.trans_aggr (
    state             STRING,
    city              STRING,
    category          STRING,
    product           STRING,
    max_amt           FLOAT64,
    min_amt           FLOAT64,
    sum_amt           FLOAT64,
    approx_cnt_cust   INT64,       -- Approximate count of distinct customers
    states_cnt        INT64,       -- Count of transactions in specific states
    mid_amt_cnt       INT64,       -- Count of transactions with amount < 100
    high_amt_cnt      INT64        -- Count of transactions with amount >= 100
);

-- Truncate the aggregation table before inserting new data
-- This ensures an idempotent load for the aggregation table, clearing previous data.
TRUNCATE TABLE discoveryds.trans_aggr;

-- Insert aggregated transaction data
-- This query aggregates data from the joined `consumer_full_load`, `trans_pos_part_cluster`,
-- and `trans_mobile_autopart_2023` tables.
INSERT INTO discoveryds.trans_aggr
SELECT
    state,
    city,
    category,
    product,
    MAX(amount) AS max_amt,
    MIN(amount) AS min_amt,
    SUM(amount) AS sum_amt,
    APPROX_COUNT_DISTINCT(t.custno) AS approx_cnt_cust, -- Approximates distinct customer count for performance
    COUNTIF(state IN ('Nevada', 'Texas', 'Oregon')) AS states_cnt, -- Counts transactions in specific states
    COUNTIF(amount < 100) AS mid_amt_cnt,                           -- Counts transactions with amount less than 100
    COUNTIF(amount >= 100) AS high_amt_cnt                          -- Counts transactions with amount 100 or more
FROM (
    SELECT
        t.txnno,
        c.custno,
        c.age,
        c.yearofbirth,
        c.profession,
        t.amount,
        t.category,
        t.product,
        t.city,
        t.state,
        t.spendby,
        t23.net,      -- Mobile network from 2023 mobile transactions
        t23.provider, -- Mobile provider from 2023 mobile transactions
        t23.activity, -- Mobile activity from 2023 mobile transactions
        t.txndt
    FROM
        curatedds.consumer_full_load AS c          -- Consumer master data
    INNER JOIN
        curatedds.trans_pos_part_cluster AS t      -- POS transactions (partitioned and clustered)
    ON
        c.custno = t.custno                         -- Joins on customer number
    INNER JOIN
        `curatedds.trans_mobile_autopart_2023` AS t23 -- Mobile transactions for year 2023
    ON
        t.txnno = t23.txnno                         -- Joins on transaction number
) AS t -- Alias for the joined subquery result
GROUP BY
    state,
    city,
    category,
    product;