/*
author : muralitheda
*/


EXPORT DATA
  OPTIONS (
    uri = 'gs://iz-cloud-training-project-bucket/datasets/bqexport/*.csv',
    format = 'CSV',
    overwrite = true,
    header = true,
    field_delimiter = ','
  )
AS (
  SELECT
    custno,
    txnno,
    SEARCH(category, 'Outdoor') AS outdoor_cat,
    category,
    STRPOS(category, 'R') AS strpos_cat,
    RPAD(category, 30, ' ') AS rpad_cat,
    REVERSE(category) AS rev_cat,
    LENGTH(category) AS len_cat,
    amount,
    ROW_NUMBER() OVER(PARTITION BY custno ORDER BY amount) AS rownum_amt,
    RANK() OVER(PARTITION BY custno ORDER BY amount) AS rnk_amt,
    DENSE_RANK() OVER(PARTITION BY custno ORDER BY amount) AS densernk_amt,
    CUME_DIST() OVER(PARTITION BY custno ORDER BY amount) AS cumedist_amt,
    FIRST_VALUE(amount) OVER(PARTITION BY custno ORDER BY amount) AS first_trans_amt,
    NTH_VALUE(amount, 3) OVER(PARTITION BY custno ORDER BY amount) AS third_highest_trans,
    LEAD(amount) OVER(PARTITION BY custno ORDER BY amount) AS next_trans,
    LAG(amount) OVER(PARTITION BY custno ORDER BY amount) AS prev_trans
  FROM
    `curatedds.trans_pos_part_cluster`
  WHERE
    loaddt = CURRENT_DATE()
);