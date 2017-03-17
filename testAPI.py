# -*- coding: utf-8 -*-
"""
Created on Wed Mar  8 18:02:07 2017

@author: Susie
"""

account_name='blobsensordata'
account_key='zUYv9mIC9KPr/k+Sa15y4mN6mtozuJcF/n979cqojT4HaMUj3ahEHaPBVtpDihwfO78JTk8sQ29xCaxGWfjtSA=='

from azure.storage.blob import BlockBlobService
import pandas as pd
from io import StringIO
from datetime import date, timedelta
from datetime import datetime
from decimal import Decimal

#from azure.storage.blob import PublicAccess
blob_service = BlockBlobService(account_name=account_name, account_key = account_key)

# for HDB result
container_name = 'hdbwakeupsleeptime'
today = date.today()  
today = today.strftime('%Y-%m-%d')
Yst = date.today() - timedelta(1)
Yst = Yst.strftime('%Y-%m-%d')

blob_string = blob_service.get_blob_to_text(container_name=container_name, blob_name=today)
blob_class = blob_string.content
blob_unicode= pd.read_csv(StringIO(blob_class),low_memory=False)
key = blob_unicode.columns.values[0]
blob_list = blob_unicode[key].tolist()

blob_split = []
for obj in blob_list:
    blob_split.append(obj.split(' '))
#for obj in blob_split:
#    if len(obj)!=4:
#        blob_split.remove(obj)

blob_frame = pd.DataFrame(blob_split)
blob_frame = blob_frame.rename(index=str, columns={2: "hubid"})
blob_frame['sleep'] = blob_frame[3]+' '+blob_frame[4]
blob_frame['wakeup'] = blob_frame[5]+' '+blob_frame[6]
#blob_frame = blob_frame.drop(blob_frame.columns[0], axis=1)
blob_frame = blob_frame[['hubid','sleep','wakeup']]

# for adventis result
container_name = 'sleepwakeuptime'
blob_string = blob_service.get_blob_to_text(container_name=container_name, blob_name=today)
blob_class = blob_string.content
blob_unicode= pd.read_csv(StringIO(blob_class),low_memory=False)
key = blob_unicode.columns.values[0]
blob_list = blob_unicode[key].tolist()
blob_split = []
for obj in blob_list:
    blob_split.append(obj.split(' '))
for obj in blob_split:
    if (len(obj)!=8)&(len(obj)!=7):
        blob_split.remove(obj)

blob_frame_adv = pd.DataFrame(blob_split)
if blob_frame_adv.shape[1]==8:
   blob_frame_adv = blob_frame_adv.rename(index=str, columns={ 2: "hubid"})#, 4:"sleep", 7:"wakeup"})
   blob_frame_adv['sleep'] = blob_frame_adv[3]+ ' '+ blob_frame_adv[4]
   blob_frame_adv['wakeup'] = blob_frame_adv[6]+ ' '+ blob_frame_adv[7]
if blob_frame_adv.shape[1]==7:
   #blob_frame_adv = blob_frame_adv[[2,4,6]]
   blob_frame_adv = blob_frame_adv.rename(index=str, columns={ 2: "hubid"})
   blob_frame_adv['sleep'] = blob_frame_adv[3]+ ' '+ blob_frame_adv[4]
   blob_frame_adv['wakeup'] = blob_frame_adv[5]+ ' '+ blob_frame_adv[6]

blob_frame_adv = blob_frame_adv[['hubid','sleep','wakeup']]

if blob_frame_adv.shape[0]!=0:
    blob_frame = blob_frame.append(blob_frame_adv)

import requests
api = 'https://portal-staging.silverline.mobi/updateSleepInfo'
out = blob_frame.to_json(orient='records')[1:-1]
out = out.replace('},{' , '}},{{')
out = out.split('},{')

test = []
for obj in out:
    test.append(obj.split(","))
    

import json
yalmfile = []
for strings in out:
    yalmfile.append(json.loads(strings))
r = requests.post(api, json = yalmfile)
