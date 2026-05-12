select *
    , case when card_category in ('Signature','Infinite') then True
        else False
        end as IS_TOP_CARD
from {{ref('stg_dim_card')}}