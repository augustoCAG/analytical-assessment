'''
The data expansion should obey the business rules:
- It does not invent transactions for players who should have none.
  Many new players will be created with no transactions (that's acceptable).
- It ensures transactions are only created for KYC-approved players and their
  timestamps are after the player's created_at.
- Affiliate redemptions are only set for affiliates that have exactly one
  player referencing them (1:1).
- IDs remain unique (script uses max(id)+1 incrementing).
'''

# Import libraries
import os
import random
import pandas as pd
import numpy as np
from faker import Faker
#from datetime import timedelta

# Set random seed for reproducibility.
# It ensures that any random operations produce the same results each time the code is run.
fake = Faker()
random.seed(42)
np.random.seed(42)

DATA_DIR = '/Users/augustocardosoagostini/Desktop/Outros Documentos/Documentos Gerais/trainings/dbt_airflow/project/analytical-assessment/technical-assessment/data'
#os.makedirs(DATA_DIR, exist_ok=True)
TARGET = 1000  # target number of rows for each table

# Input files (output from part_1_task_1__preprocess_data.ipynb)
players_in = os.path.join(DATA_DIR, "players_cleaned_from_notebook.csv")
affiliates_in = os.path.join(DATA_DIR, "affiliates_cleaned_from_notebook.csv")
tx_in = os.path.join(DATA_DIR, "transactions_cleaned_from_notebook.csv")

# Load cleaned data ensuring date parsing
players = pd.read_csv(players_in, parse_dates=['created_at','updated_at'])
affiliates = pd.read_csv(affiliates_in, parse_dates=['redeemed_at'])
transactions = pd.read_csv(tx_in, parse_dates=['timestamp'])

# Ensure types for consistency for IDs, bools, amounts (timestamps were already parsed)
players['id'] = players['id'].astype('int64')
players['affiliate_id'] = players['affiliate_id'].astype('Int64')  # Int64 (with upper case "I") is nullable
players['is_kyc_approved'] = players['is_kyc_approved'].astype('bool')
affiliates['id'] = affiliates['id'].astype('int64')
transactions['id'] = transactions['id'].astype('int64')
transactions['player_id'] = transactions['player_id'].astype('int64')
transactions['amount'] = transactions['amount'].astype('float64')


# 1) Expand players to TARGET rows
cur_player_max = int(players['id'].max())
countries = ['US','GB','DE','BR','CA','FR','IN','ES','NL','IT','AU','MX','JP','AR','UY']
aff_ids = affiliates['id'].astype(int).tolist()

new_players = []
while len(players) + len(new_players) < TARGET:
    cur_player_max += 1
    created = fake.date_time_between(start_date='-3y', end_date='now', tzinfo=None)
    updated = fake.date_time_between(start_date=created, end_date='now', tzinfo=None)
    # assign affiliate to some players (80% use affiliate - following original distribution)
    if random.random() < 0.8:
        aff = int(np.random.choice(aff_ids))
    else:
        aff = pd.NA
    is_kyc = random.random() < 0.80  # ~80% KYC approved - following original distribution
    new_players.append({
        'id': cur_player_max,
        'affiliate_id': aff,
        'country_code': random.choice(countries),
        'is_kyc_approved': is_kyc,
        'created_at': created,
        'updated_at': updated
    })

players_expanded = pd.concat([players, pd.DataFrame(new_players)], ignore_index=True)
players_expanded['affiliate_id'] = players_expanded['affiliate_id'].astype('Int64')

# Force all datetime columns to be tz-naive BEFORE concatenation - this prevents mixing errors
# Fix players datetimes
if not players['created_at'].dt.tz is None:
    players['created_at'] = players['created_at'].dt.tz_localize(None)
if not players['updated_at'].dt.tz is None:
    players['updated_at'] = players['updated_at'].dt.tz_localize(None)
# Now concatenate (all should be tz-naive)
players_expanded = pd.concat([players, pd.DataFrame(new_players)], ignore_index=True)
players_expanded['affiliate_id'] = players_expanded['affiliate_id'].astype('Int64')
# Fix other datetime columns
if not transactions['timestamp'].dt.tz is None:
    transactions['timestamp'] = transactions['timestamp'].dt.tz_localize(None)
if not affiliates['redeemed_at'].dt.tz is None:
    affiliates['redeemed_at'] = affiliates['redeemed_at'].dt.tz_localize(None)


# 2) Expand affiliates until TARGET rows
new_affiliates = []
cur_aff_max = int(affiliates['id'].max())
while len(affiliates) + len(new_affiliates) < TARGET:
    cur_aff_max += 1
    new_affiliates.append({
        'id': cur_aff_max,
        'code': fake.lexify('??????').upper(),
        'origin': random.choice(['YouTube','Discord','Twitter','Twitch','unknown']),
        'redeemed_at': pd.NaT  # temporarily NaT
    })

affiliates_expanded = pd.concat([affiliates, pd.DataFrame(new_affiliates)], ignore_index=True)
affiliates_expanded['id'] = affiliates_expanded['id'].astype('int64')

# Affiliates linked to exactly 1 player
aff_counts = players_expanded['affiliate_id'].dropna().astype(int).value_counts()
single_affs = aff_counts[aff_counts == 1].index.tolist()

# Randomly mark 80% of these as redeemed - following original distribution
redeemed_subset = set(random.sample(single_affs, int(len(single_affs) * 0.8)))

# Assign redeemed_at = player's created_at for chosen subset
for aff_id in redeemed_subset:
    player_row = players_expanded.loc[players_expanded['affiliate_id'] == aff_id].iloc[0]
    affiliates.loc[affiliates['id'] == aff_id, 'redeemed_at'] = player_row['created_at']


# 3) Expand transactions to TARGET rows
# We will create transactions only for KYC-approved players.
cur_tx_max = int(transactions['id'].max()) if len(transactions)>0 else 0
transactions_expanded = transactions.copy()

# eligible players = KYC approved
eligible_players = players_expanded[players_expanded['is_kyc_approved']==True]['id'].astype(int).tolist()

# We will fill the transactions table to TARGET rows, but do not force every player to have txs.
while len(transactions_expanded) < TARGET:
    cur_tx_max += 1
    player = int(np.random.choice(eligible_players))
    # transaction timestamp after player created_at
    p_created = pd.to_datetime(players_expanded.loc[players_expanded['id']==player,'created_at'].iloc[0])
    # choose a timestamp between p_created and now
    max_days = (pd.Timestamp.now(tz=None) - p_created).days
    if max_days <= 0:
        ts = p_created + pd.Timedelta(seconds=60)
    else:
        ts = p_created + pd.Timedelta(days=random.randint(0, max_days), seconds=random.randint(0, 86400))
    # 20% deposits, 80% withdrawals - following original distribution
    tx_type = random.choices(['Deposit','Withdraw'], weights=[0.20,0.80])[0]
    # typical amounts in range 140 to 800+ (mostly 140-400) - following original distribution
    amount = round(float(np.random.exponential(scale=180) + 140), 2) 
    transactions_expanded = pd.concat([transactions_expanded, pd.DataFrame([{
        'id': cur_tx_max,
        'timestamp': ts,
        'player_id': player,
        'type': tx_type,
        'amount': amount
    }])], ignore_index=True)


# 4) Final integrity checks

# IDs must be unique - keep last in case of any issues
players_expanded = players_expanded.drop_duplicates('id', keep='last').reset_index(drop=True)
affiliates_expanded = affiliates_expanded.drop_duplicates('id', keep='last').reset_index(drop=True)
transactions_expanded = transactions_expanded.drop_duplicates('id', keep='last').reset_index(drop=True)

# Ensure transactions only reference existing players
transactions_expanded = transactions_expanded[transactions_expanded['player_id'].isin(players_expanded['id'])]


# 5) Write outputs
players_expanded.to_csv(os.path.join(DATA_DIR, "players_expanded.csv"), index=False)
affiliates_expanded.to_csv(os.path.join(DATA_DIR, "affiliates_expanded.csv"), index=False)
transactions_expanded.to_csv(os.path.join(DATA_DIR, "transactions_expanded.csv"), index=False)


print("Wrote players_expanded.csv, affiliates_expanded.csv, transactions_expanded.csv")
print("Counts:", len(players_expanded), len(affiliates_expanded), len(transactions_expanded))
