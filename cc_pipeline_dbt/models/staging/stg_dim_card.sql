select 
    "card_category" as card_category,
    "card_provider" as card_provider,
    "cc_num" as cc_num,
    "credit_limit" as credit_limit,
    "expiration_date" as expiration_date,
    "issue_date" as issue_date,
    
from {{source('staging','slv_dim_card')}}