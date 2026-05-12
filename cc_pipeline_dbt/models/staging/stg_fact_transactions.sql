select  
    "cust_uuid" as cust_uuid,
    "trans_date_trans_time" as trans_date_trans_time,
    "is_fraud" as is_fraud,
    "merch_uuid" as merch_uuid,
    "amt" as amt,
    "cc_num" as cc_num,
    "category" as category,
    "trans_num" as trans_num,
    "unix_time" as unix_time
from {{source('staging','slv_fact_transactions')}}