# -*- coding: utf-8 -*-
"""
Created on Mon Dec 23 08:23:31 2019

@author: gerold
"""

import pandas as pd
import numpy as np
import sqlite3

denormalized = pd.read_csv('./data-repo/cleaned/denormalized.csv')

# Just realized that funder and installer contain both 0 an nan values. 
# Let's convert nulls to nan
denormalized[['funder', 'installer']] = denormalized[['funder', 'installer']].replace({'0': np.nan})

# Create fact table with isnpection data
fact = denormalized[['inspection_id', 'date_recorded', 'ws_id', 'status_group']].copy()

# Create waterpoint dimension table
wpt_dim = denormalized[['ws_id', 'wpt_name', 'region',
                      'latitude', 'longitude','funder',
                      'installer', 'basin', 'construction_year',
                      'distance']].copy()

# Delete dups due to more inspections but same waterpoint dimension results
wpt_dim.drop_duplicates(inplace=True)

dups = wpt_dim.groupby('ws_id')['region'].count()
dups = dups[dups>1]

# Extract waterpoint which attributes change
changing = wpt_dim.loc[wpt_dim['ws_id'].isin(dups.index.to_list())].copy()
changing.sort_values('ws_id', inplace=True)

# These are very few in number, so simplification will be applied.
# I will keep the last obs. for eeach obs. grouped by waterpoints
wpt_dim.drop_duplicates(subset=['ws_id'], keep='last', inplace=True)

# Create pump dimension table
pump_dim = denormalized[['ws_id', 'extraction_type_class', 'quality_group',
                       'waterpoint_type_group']].copy()

pump_dim.drop_duplicates(inplace=True)

# Extract pumps which attributes change
dups = pump_dim.groupby('ws_id')['quality_group'].count()
dups = dups[dups>1]
# There are none, so we are good

# Create admin dimension tables
admin_dim = denormalized[['ws_id', 'payment']].copy()
admin_dim.drop_duplicates(inplace=True)
# No dups


## Write normalized data


admin_dim.to_csv('data-repo/cleaned/normalized/admin.csv', index=False)
fact.to_csv('data-repo/cleaned/normalized/fact.csv', index=False)
pump_dim.to_csv('data-repo/cleaned/normalized/pump.csv', index=False)
wpt_dim.to_csv('data-repo/cleaned/normalized/wpt.csv', index=False)


## Create and write into database


# This code is mainly taken from: https://datatofish.com/create-database-python-using-sqlite3/
conn = sqlite3.connect('data-repo/cleaned/normalized/PumpDB')
c = conn.cursor()

# Create table - FACT
c.execute('''CREATE TABLE FACT
             ([inspection_id] INTEGER PRIMARY KEY,[date_recorded] date, 
             [ws_id] INTEGER, [status_group] text)''')
          
# Create table - ADMIN
c.execute('''CREATE TABLE ADMIN
             ([ws_id] INTEGER PRIMARY KEY,[payment] text)''')
        
# Create table - PUMP
c.execute('''CREATE TABLE PUMP
             ([ws_id] INTEGER PRIMARY KEY, [extraction_type_class] text,
             [quality_group] text ,[waterpoint_type_group] text)''')

# Create table - WPT
c.execute('''CREATE TABLE WPT
          ([ws_id] INTEGER PRIMARY KEY, [wpt_name] text, [region] text,
          [latitude] REAL, [longitude] REAL, [funder] text, [installer] text,
          [basin] text, [construction_year] year, [distance] REAL)''')                 

conn.commit()

# Note that the syntax to create new tables should only be used once in the code (unless you dropped the table/s at the end of the code). 
# The [generated_id] column is used to set an auto-increment ID for each record
# When creating a new table, you can add both the field names as well as the field formats (e.g., Text)

# Fill the database
fact.to_sql('FACT', conn, if_exists='append', index = False)
admin_dim.to_sql('ADMIN', conn, if_exists='append', index=False)
pump_dim.to_sql('PUMP', conn, if_exists='append', index=False)
wpt_dim.to_sql('WPT', conn, if_exists='append', index=False)

