select *
    , DATEDIFF('year',DOB,CURRENT_DATE()) as age
from {{ref('stg_dim_customer')}}