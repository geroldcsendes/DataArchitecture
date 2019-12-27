# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 15:38:40 2019

@author: gerold
"""

import pandas as pd
import numpy as np

### Read in drivendata data ### 


## Train data
# features
features = pd.read_csv("data-repo/raw/drivendata/train-features.csv")

# Drop columns that are recodings
features.drop(['quantity_group', 'payment_type', 'recorded_by'], axis=1, inplace=True)

# labels
labels = pd.read_csv("data-repo/raw/drivendata/train-labels.csv")

# Get dimension of the data
data_dimension = features.shape # 59400 obs, 38 vars (after dropping)


### Data quality checks ###


# Check unique regions
unique_regions = features['region'].sort_values().unique()
region_coding = features.groupby('region')['region_code'].nunique()

# So the coding is not ok, let's drop it, nor can we use the district_code var, drop it too
features.drop(['district_code', 'region_code'], axis=1, inplace=True)

# Check geodata by wpt_name
check_coords = features.groupby('wpt_name')['longitude'].nunique()
check_coords = check_coords[check_coords >1]


### Restrict dataset that is relevant to the DS assignment
rel_vars =  ["id", "date_recorded", "latitude", "longitude", "funder", 'installer', 'basin', 'region', 'lga', 'ward', 'construction_year',
             "payment", "extraction_type_class", "quality_group" , "waterpoint_type_group", "wpt_name"]
features = features[rel_vars]

### Recode missing data in features ###
features['construction_year'].replace({0:np.nan}, inplace=True)
features['payment'].replace({'unknown':np.nan}, inplace=True)
features['quality_group'].replace({'unknown':np.nan}, inplace=True)


### Filter where wpt_name is not known
features = features.loc[features['wpt_name']!='none'].copy()

# latitude=-2e-08,longitude = 0 seems to be unknown data, drop it 
features = features.loc[(features['latitude'] != -2.000000e-08) & (features['longitude'] != 0.0)]


# Since there is no id for a water station, I will give artifically create one for every lat-long pair
# Thus I am assuming that there is only one station per location
# Let's check whether this holds true
check_station = features.drop_duplicates(subset = ['latitude', 'longitude'])
check_dups = check_station.groupby(['latitude', 'longitude'])['wpt_name'].nunique()
check_dups = check_dups[check_dups > 1]
# There aren't more owners in a location so we are good

# Give ids
water_stations = check_station[['latitude', 'longitude']].copy()
water_stations.reset_index(inplace = True)
water_stations.rename(columns = {'index': 'ws_id'}, inplace = True)

# Join ws_ids to features set
features = features.merge(water_stations, on = ('latitude', 'longitude'), how='left')

# Join features to labels
output = features.merge(labels, on='id', how='inner')

# Rename id to inspection id
output.rename(columns={'id':'inspection_id'}, inplace=True)

# Check how many with more inspections
more_insp = output['ws_id'].value_counts()
len(more_insp[more_insp > 1]) # Only 69

# Write to csv
output.to_csv('data-repo/raw/refine_prep.csv', index = False)


### Join output to economic to create denormalized.csv


economic = pd.read_csv('data-repo/cleaned/economic.csv')
denormalized = output.merge(economic, on='region', how='left')

# Join is successful
mytry = denormalized.loc[denormalized['gdp_percap'].isna()]['region']


## Read in and join distances 


denormalized = denormalized.merge(distances[['latitude', 'longitude', 'distance']], 
                                  on = ('latitude', 'longitude'), how='left')

# Write denormalized
denormalized.to_csv('data-repo/cleaned/denormalized.csv', index=False)



