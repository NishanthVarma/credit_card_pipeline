select f.* 
    , dcust.first
    , dcust.last
    , dm.merchant
    , dc.card_provider
    , dc.credit_limit
from {{ref('stg_fact_transactions')}} as f
join {{ref('stg_dim_card')}} as dc
    on dc.cc_num = f.cc_num
join {{ref('stg_dim_customer')}} as dcust
    on dcust.cust_uuid = f.cust_uuid
join {{ref('stg_dim_merchant')}} as dm
    on dm.merch_uuid = f.merch_uuid