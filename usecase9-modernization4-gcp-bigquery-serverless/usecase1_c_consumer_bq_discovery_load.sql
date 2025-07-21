/*
author : muralitheda
*/

-- Create discoveryds schema if it does not exist, with location 'us'
CREATE SCHEMA IF NOT EXISTS discoveryds
OPTIONS(location='us');

-- Create discoveryds.consumer_trans_pos_mob_online table
CREATE TABLE IF NOT EXISTS discoveryds.consumer_trans_pos_mob_online (
    txnno               NUMERIC,
    txndt               DATE,
    custno              INT64,
    fullname            STRING,
    age                 INT64,
    profession          STRING,
    trans_day           STRING,
    trans_type          STRING,
    net                 STRING,
    online_pos_amount   FLOAT64,
    geo_coordinate      GEOGRAPHY,
    provider            STRING,
    activity            STRING,
    spendby             STRING,
    city                STRING,
    state               STRING,
    online_pos_category STRING,
    product             STRING,
    loadts              TIMESTAMP,
    loaddt              DATE
);

-- Data Wrangling: Joining, widening, and denormalizing data using Common Table Expressions (CTEs)
-- CTEs (with clause queries) improve readability and reusability of complex queries.
-- They are processed once and the result is cached in memory, which is beneficial for smaller data volumes.
-- This section focuses on enriching data, ensuring data coverage (handling nulls), and stitching together
-- consumer events across multiple channels (POS, Mobile, Online).
-- The joins standardize and create a wide, denormalized table.

INSERT INTO discoveryds.consumer_trans_pos_mob_online

WITH
-- CTE to join Mobile and POS transactions
pos_mob_trans AS (
    SELECT
        mob.txnno AS mob_txnno,
        pos.txnno AS pos_txnno,
        CASE
            WHEN mob.dt = pos.txndt THEN 'Same Day Trans'
            ELSE 'Multi Day Trans'
        END AS trans_day,
        COALESCE(mob.dt, pos.txndt) AS txndt,
        CASE
            WHEN mob.txnno IS NULL THEN 'POS'
            WHEN pos.txnno IS NULL THEN 'MOB'
            ELSE 'POS_MOB'
        END AS trans_type,
        mob.net,
        pos.amount,
        mob.geo_coordinate,
        mob.provider,
        CASE
            WHEN mob.activity = 'STILL' THEN 'In Store Pickup'
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
        `curatedds.trans_mobile_autopart_20*` AS mob -- Wildcard table query for mobile transactions across years
    FULL JOIN
        `curatedds.trans_pos_part_cluster` AS pos
    ON
        mob.txnno = pos.txnno
),
-- CTE to join Customer dimension with Online transactions
cust_online_trans AS (
    SELECT
        *,
        CASE
            WHEN transsk IS NOT NULL THEN 'online'
            ELSE NULL
        END AS trans_type
    FROM
        `curatedds.consumer_full_load` AS cust
    LEFT JOIN
        `curatedds.trans_online_part` AS trans
    ON
        cust.custno = trans.customerid
)
-- Final SELECT statement to combine all transaction types and customer data
SELECT
    COALESCE(mob_txnno, pos_txnno) AS txnno,
    trans.txndt,
    trans.custno,
    fullname,
    age,
    profession,
    trans_day,
    COALESCE(cust.trans_type, trans.trans_type) AS trans_type,
    COALESCE(net, 'na') AS net,
    COALESCE(productprice, COALESCE(amount, 0.0)) AS online_pos_amount,
    geo_coordinate,
    COALESCE(provider, 'na') AS provider,
    COALESCE(activity, 'na') AS activity,
    COALESCE(spendby, 'na') AS spendby,
    COALESCE(city, 'unknown') AS city,
    COALESCE(state, 'unknown') AS state,
    COALESCE(productcategory, category) AS online_pos_category,
    product,
    COALESCE(pos_loadts, mob_loadts) AS loadts,
    COALESCE(pos_loaddt, mob_loaddt) AS loaddt
FROM
    cust_online_trans AS cust
LEFT OUTER JOIN
    pos_mob_trans AS trans
ON
    trans.custno = cust.custno;

-- Create discoveryds.trans_aggr table for aggregated transaction data
CREATE TABLE IF NOT EXISTS discoveryds.trans_aggr (
    state             STRING,
    city              STRING,
    category          STRING,
    product           STRING,
    max_amt           FLOAT64,
    min_amt           FLOAT64,
    sum_amt           FLOAT64,
    approx_cnt_cust   INT64,
    states_cnt        INT64,
    mid_amt_cnt       INT64,
    high_amt_cnt      INT64
);

-- Truncate the aggregation table before inserting new data
TRUNCATE TABLE discoveryds.trans_aggr;

-- Insert aggregated transaction data
INSERT INTO discoveryds.trans_aggr
SELECT
    state,
    city,
    category,
    product,
    MAX(amount) AS max_amt,
    MIN(amount) AS min_amt,
    SUM(amount) AS sum_amt,
    APPROX_COUNT_DISTINCT(t.custno) AS approx_cnt_cust,
    COUNTIF(state IN ('Nevada', 'Texas', 'Oregon')) AS states_cnt,
    COUNTIF(amount < 100) AS mid_amt_cnt,
    COUNTIF(amount >= 100) AS high_amt_cnt
FROM (
    SELECT
        t.txnno,
        c.custno,
        age,
        yearofbirth,
        profession,
        amount,
        category,
        product,
        city,
        state,
        spendby,
        net,
        provider,
        activity,
        t.txndt
    FROM
        curatedds.consumer_full_load AS c
    INNER JOIN
        curatedds.trans_pos_part_cluster AS t
    ON
        c.custno = t.custno
    INNER JOIN
        `curatedds.trans_mobile_autopart_2023` AS t23
    ON
        t.txnno = t23.txnno
) AS t
GROUP BY
    state,
    city,
    category,
    product;
