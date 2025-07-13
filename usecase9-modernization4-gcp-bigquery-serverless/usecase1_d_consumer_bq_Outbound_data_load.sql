EXPORT DATA
  OPTIONS (
    uri = 'gs://inceptez-common-bucket/dataset/bqexport/*.csv',
    format = 'CSV',
    overwrite = true,
    header = true,
    field_delimiter = ',')
AS (
select custno,txnno,search(category,'Outdoor') as outdoor_cat,category,strpos(category,'R') strpos_cat,rpad(category,30,' ') rpad_cat,reverse(category) rev_cat,
length(category) len_cat,
amount,
row_number() over(partition by custno order by amount) rownum_amt ,
rank() over(partition by custno order by amount) rnk_amt ,
dense_rank() over(partition by custno order by amount) densernk_amt ,
cume_dist() over(partition by custno order by amount) cumedist_amt,
first_value(amount) over(partition by custno order by amount) first_trans_amt,
nth_value(amount,3) over(partition by custno order by amount) third_highest_trans,
lead(amount) over(partition by custno order by amount) next_trans,
lag(amount) over(partition by custno order by amount) prev_trans,
 from `curatedds.trans_pos_part_cluster`
 where loaddt=current_date()
 );
