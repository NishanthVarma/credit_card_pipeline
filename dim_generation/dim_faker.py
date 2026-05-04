import sys
sys.path.append(r'N:\DE\CC Transaction Pipeline')

import pandas as pd
from faker import Faker
import os
import random
from datetime import timedelta
import numpy as np
import cfg.config as config



fake = Faker()

df1 = pd.read_csv(os.path.join(config.SOURCE_DIR, 'fraudTrain.csv'))

df2 = pd.read_csv(os.path.join(config.SOURCE_DIR, 'fraudTest.csv'))

df = pd.concat([df1,df2])
df = df.drop(df.columns[0],axis=1)
df = df.drop_duplicates()

#dim customers

df_cust = df[['cc_num', 'first', 'last', 'gender', 'street', 'city', 'state', 'zip', 'lat', 'long', 'city_pop', 'job', 'dob']].drop_duplicates().copy()

#observed 999 unique cc_nums and corresponding 999 unique customer details

df_cust['cust_uuid'] = [fake.unique.uuid4() for _ in range(len(df_cust))]
# df_cust = df_cust.drop('cc_num',axis=1)

#dim merchant

df_sorted = df.sort_values(by=['merchant', 'trans_date_trans_time'], ascending=[True, False])
df_merc = df_sorted.drop_duplicates(subset='merchant', keep='first')
df_merc = df_merc[['merchant', 'merch_lat', 'merch_long']].copy()

df_merc['merch_uuid'] = [fake.unique.uuid4() for _ in range(len(df_merc))]

#dim card

df_card = df[['cc_num']].drop_duplicates().copy()

def get_category_limit(category):
    
    tiers = {
        'Basic':     {'range': (1000, 5000),   'step': 500},
        'Gold':      {'range': (5000, 15000),  'step': 1000},
        'Platinum':  {'range': (15000, 30000), 'step': 2500},
        'Signature': {'range': (30000, 50000), 'step': 5000},
        'Infinite':  {'range': (50000, 100000),'step': 10000}
    }
    
    config = tiers.get(category)
    
    limit = random.randrange(config['range'][0], config['range'][1] + 1, config['step'])
    return limit


def generate_card_details(row):
    card_provider = fake.credit_card_provider()
    
    category = random.choice(['Basic', 'Gold', 'Platinum', 'Signature', 'Infinite'])
    
    issue_date = fake.date_between(start_date='-5y', end_date='today')
    
    expiry_years = random.randint(3, 5)
    expiration_date = issue_date + timedelta(days=expiry_years * 365)
    
    limit = get_category_limit(category)
    
    return pd.Series([card_provider, category, expiration_date, issue_date, limit])

df_card[['card_provider', 'card_category', 'expiration_date', 'issue_date', 'credit_limit']] = df_card.apply(generate_card_details, axis=1)


# fact revamp

df_joined = pd.merge(df, df_merc[['merchant','merch_uuid']], on='merchant', how='left')

df_joined = pd.merge(df_joined, df_cust[['cc_num','cust_uuid']], on='cc_num', how='left')

# final_df = df_joined[['trans_num', 'merch_uuid', 'cust_uuid', 'trans_date_trans_time', 'cc_num', 'category', 'amt', 'unix_time', 'is_fraud']].copy()

# write to csv

df_cust = df_cust.drop('cc_num',axis=1)

df_cust.to_csv(os.path.join(config.LANDING_DIR, 'dim_customer.csv'),index=False)
df_merc.to_csv(os.path.join(config.LANDING_DIR, 'dim_merchant.csv'),index=False)
df_card.to_csv(os.path.join(config.LANDING_DIR, 'dim_card.csv'),index=False)
df_joined.to_csv(os.path.join(config.LANDING_DIR, 'fact_transactions.csv'),index=False)