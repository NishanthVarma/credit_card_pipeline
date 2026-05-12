select  
    "merch_long" as merch_long,
    "merch_uuid" as merch_uuid,
    "merch_lat" as merch_lat,
    "merchant" as merchant
from {{source('staging','slv_dim_merchant')}}