#!/usr/bin/env python
# coding: utf-8

# # import pandas as pd
# import numpy as np
# import json
# import requests

# # Geocoder

# In[2]:


####### Települések geokódolása #######
import geocoder
import pandas as pd
import time
import numpy as np
import json
import requests


# In[3]:


def read_abt(path):
    """
    (Source) Excel beolvasása df-be, külön df-be csak az irsz-ek, amit ledistinctelünk, hogy ne legyen felesleges lekérdezés
    Return: distinct_tav: dinstinct irsz df, source: source df
    """
    source = pd.read_csv(path)
    
    distinct_tav = source[['irsz', 'munka_irsz']].copy()
    
    distinct_tav.drop_duplicates(inplace=True)
    
    return distinct_tav, source


# In[168]:


def get_coords(df):

    irsz_types = ['irsz'] #, 'munka_irsz'
    
    for irsz_type in irsz_types:
        
        print(irsz_type)
        
        irsz_list = df[irsz_type].tolist()
        #irsz_list = irsz_list[]

        orszag = 'Hungary'

        coord_dict = {}

        for irsz in irsz_list:
            print(int(irsz))

            try:

                insert_address = str(int(irsz)) +','+ orszag
                g = geocoder.here(insert_address,
                                   app_id='9s40bPAzWV12JBwgCLnj',
                                   app_code='qlG3zbGQVQ5IlMY0FEAiRQ')

                coord = str(g.json['lat']) + ',' + str(g.json['lng'])
                
                coord_dict[irsz] = coord

            except:

                try:

                    insert_address = str(int(irsz))[:2] + '00' +','+ orszag

                    g = geocoder.here(insert_address,
                                   app_id='9s40bPAzWV12JBwgCLnj',
                                   app_code='qlG3zbGQVQ5IlMY0FEAiRQ')
                    
                    coord = str(g.json['lat']) + ',' + str(g.json['lng']) 
                    
                    coord_dict[irsz] = coord

                except:
                    coord_dict[irsz] = ''+''
                    print('not_ok')

            #time.sleep(2)
            print(coord)

        colname_coord = irsz_type + '_coord'

        df[colname_coord] = df[irsz_type].map(coord_dict)
        
        coord_dict = {}
        irsz_list = []

    return df    


# In[5]:


def get_dist(df):
    
    df['tav'] = np.nan
    
    for index, row in df.iterrows():
        
        from_geo = row['irsz_coord']
        to_geo = row['munka_irsz_coord']
        url = 'https://route.api.here.com/routing/7.2/calculateroute.json?app_id={app_id}&app_code={app_code}&waypoint0=geo!{from_geo}&waypoint1=geo!{to_geo}&mode=fastest;car;traffic:disabled&departure_time={dep_time}'.format(app_id='9s40bPAzWV12JBwgCLnj', app_code='qlG3zbGQVQ5IlMY0FEAiRQ',from_geo=from_geo, to_geo=to_geo, dep_time='2019-04-16T08:00:0')

        resp = json.loads(requests.get(url).content)

        #Check if response
        if 'response' in resp:
            status = 'OK'
            distance = resp.get('response').get('route')[0].get('summary').get('distance')
            trafficTime = resp.get('response').get('route')[0].get('summary').get('trafficTime')
            baseTime = resp.get('response').get('route')[0].get('summary').get('baseTime')
            travelTime =  resp.get('response').get('route')[0].get('summary').get('travelTime')
        else:
            status = 'ERROR'
            distance = ''
            trafficTime = ''
            baseTime = ''
            travelTime = ''
        
        df.loc[index,'tav'] = distance
        
    return df


# #### ABT-re

# In[24]:


path = r'G:\Saját meghajtó\Common\Projekt\EON\20190502_Kormenedzsment\Adatok\ABT\ABT1205.csv'

tav, source = read_abt(path)


# In[25]:


tav


# In[26]:


tav = get_coords(tav)


# In[27]:


tav


# In[29]:


tav.to_csv(r'G:\Saját meghajtó\Common\Projekt\EON\20190502_Kormenedzsment\Adatok\ABT_check\extensions\tav.csv')


# In[36]:


tav = pd.read_csv(r'G:\Saját meghajtó\Common\Projekt\EON\20190502_Kormenedzsment\Adatok\ABT_check\extensions\tav.csv',
                 usecols = [1,2,3,4,5])


# In[43]:


tav


# In[40]:


tav.loc[tav.tav.isna()]


# In[41]:


tav_test = tav.drop_duplicates(subset = ['irsz', 'munka_irsz'])


# In[45]:


tav_test.drop('tav', axis = 1, inplace = True)


# In[46]:


tav_test


# In[ ]:





# In[47]:


get_dist(tav)


# In[48]:


tav


# In[49]:


tav.to_csv(r'G:\Saját meghajtó\Common\Projekt\EON\20190502_Kormenedzsment\Adatok\ABT_check\extensions\tav_dist.csv')


# ### Get tavolsag for missing irsz and munka irsz combinations

# In[102]:


tav = pd.read_csv(r'G:\Saját meghajtó\Common\Projekt\EON\20190502_Kormenedzsment\Adatok\ABT_check\extensions\tav_missing.csv')


# In[103]:


tav = tav[['irsz', 'munka_irsz']].copy()


# In[104]:


tav.drop_duplicates(inplace=True)


# In[105]:


tav.info()


# In[294]:


distances = pd.read_csv(r'G:\Saját meghajtó\Common\Projekt\EON\20190502_Kormenedzsment\Adatok\ABT_check\extensions\tav_dist.csv',
                       usecols = [1,2,3,4,5])


# In[295]:


distances


# In[296]:


distances = distances.merge(res2, on='irsz', how='left', suffixes=('', '_corr'))


# In[297]:


def fix_coords(x):
    
    if type(x['irsz_coord']) != str:
        return x['irsz_coord_corr']
    else:
        return x['irsz_coord']


# In[298]:


distances


# In[299]:


distances.info()


# In[300]:


distances['irsz_coord'] = distances.apply(lambda x: fix_coords(x), axis=1)


# In[301]:


distances.loc[distances['irsz_coord'].isna()]


# In[302]:


distances_todo = distances.loc[distances['irsz'] != 0]


# In[303]:


distances_todo = distances.loc[~((distances['irsz'].isna()) | (distances['irsz'] == 0 ))]
distances_todo= distances_todo.loc[distances_todo['tav'].isna()]

distances_todo = distances_todo.drop('tav', axis=1)
distances_todo


# In[220]:


distances_finished = get_dist(distances_todo)


# In[221]:


distances_finished


# In[222]:


distances


# In[304]:


distances = distances.merge(distances_finished[['irsz_coord','munka_irsz_coord','tav']], 
                                on = ('irsz_coord','munka_irsz_coord'), suffixes=('','_corr'),
                               how='left')


# In[305]:


distances


# In[306]:


def fix_tav(x):
    
    if np.isnan(x['tav']) and x['tav_corr'] != "":
        return x['tav_corr']
    else:
        return x['tav']


# In[307]:


distances.loc[distances_try['tav'].isna()]


# In[308]:


distances['tav'] = distances.apply(lambda x: fix_tav(x), axis=1)


# In[309]:


tav_miss = distances.loc[distances['tav'].isna()]
tav_miss


# In[311]:


tav_miss = tav_miss.loc[(~tav_miss['irsz_coord'].isna()) & (~tav_miss['munka_irsz_coord'].isna())].copy()


# In[312]:


tav_miss


# In[313]:


tav_miss = tav_miss[['irsz_coord', 'munka_irsz_coord']].copy()


# In[314]:


tav_miss.drop_duplicates(inplace=True)


# In[282]:


tav_miss.to_excel(r'G:\Saját meghajtó\Common\Projekt\EON\20190502_Kormenedzsment\Adatok\ABT_check\extensions\man_calc.xlsx')


# In[277]:


tav_miss.shape


# In[278]:


manual_calc = [7400, 353000, 343000, 8400, 260, 3300, 344000]
len(manual_calc)


# In[315]:


tav_miss['tav'] = manual_calc


# In[316]:


tav_miss


# In[317]:


distances.drop(['irsz_coord_corr', 'tav_corr'], inplace=True, axis=1)


# In[318]:


distances = distances.merge(tav_miss[['irsz_coord','munka_irsz_coord','tav']], 
                                on = ('irsz_coord','munka_irsz_coord'), suffixes=('','_corr'),
                               how='left')


# In[319]:


distances


# In[320]:


distances['tav'] = distances.apply(lambda x: fix_tav(x), axis=1)


# In[321]:


distances.loc[distances['tav'].isna()]


# In[322]:


distances.to_csv(r'G:\Saját meghajtó\Common\Projekt\EON\20190502_Kormenedzsment\Adatok\ABT_check\extensions\tav_dist1209.csv',
                i)


# In[325]:


distances.to_excel(r'G:\Saját meghajtó\Common\Projekt\EON\20190502_Kormenedzsment\Adatok\ABT_check\extensions\tav_dist1209.xlsx',
                  index=False)


# In[ ]:





# In[251]:


myres


# In[ ]:




