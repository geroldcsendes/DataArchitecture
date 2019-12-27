# -*- coding: utf-8 -*-
"""
Created on Mon Dec 23 14:03:46 2019

@author: gerold
"""

## This code compares the performance of JSON, CSV and Parquet. 
## The following dimensions will be considered
##  Disk space
##  Time to save
##  Time to read

import pandas as pd
import numpy as np
import time

import json
import pyarrow

# Read in denormalized data
denormalized = denormalized = pd.read_csv('./data-repo/cleaned/denormalized.csv')

# Make this data bigger to demonstrate a real life use-case
# I am going to random sample 50,000 observations 6 time, thus, ending up with 30,000 
# obs. in total with both numeric, date and categorical data.
res_dict = {}
for i in range(6):
    
    res_dict[i] = denormalized.sample(50000, replace=True)

big_table = pd.concat(res_dict)
big_table.reset_index(inplace=True)
big_table.info() 
# Still only 54.5 MB RAM in pandas, easy to work with
# Create this for inspection
small_table = big_table[0:10].copy()

## Write time

data_lib = './data-repo/structures_comparison/'
df_csv = 'compare.csv'
df_json = 'compare.json'
df_parquet = 'comapare.parquet'

# CSV
start_time = time.time()
for i in range(10):
    big_table.to_csv(data_lib + df_csv)
end_time = time.time()
csv_write_time = (end_time - start_time) / 10 # 17.98

# JSON
start_time = time.time()
for i in range(10):
    big_table.to_json(data_lib + df_json, orient='records')
end_time = time.time()
json_write_time = (end_time - start_time) / 10 # 7.07

# Parquet
start_time = time.time()
for i in range(10):
    big_table.to_parquet(data_lib + df_parquet)
end_time = time.time()
pq_write_time = (end_time - start_time) / 10 # 1.07

## Read time

# CSV
start_time = time.time()
for i in range(10):
    temp_df = pd.read_csv(data_lib + df_csv)
end_time = time.time()
csv_read_time = (end_time - start_time) / 10 # 2.57

# JSON
start_time = time.time()
for i in range(10):
    temp_df = pd.read_json(data_lib + df_json, orient='records')
end_time = time.time()
json_read_time = (end_time - start_time) / 10 # 8.38

# Parquet
start_time = time.time()
for i in range(10):
    temp_df = pd.read_parquet(data_lib + df_parquet)
end_time = time.time()
pq_read_time = (end_time - start_time) / 10 # 0.48




    
mydf = pd.read_json()