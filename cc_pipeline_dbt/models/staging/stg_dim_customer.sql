select  
    "zip" as zip,
    "long" as long,
    "job" as job,
    "first" as first,
    "cust_uuid" as cust_uuid,
    "state" as state,
    "city" as city,
    "dob" as dob,
    "gender" as gender,
    "last" as last,
    "street" as street,
    "city_pop" as city_pop,
    "lat" as lat
from {{source('staging','slv_dim_customer')}}