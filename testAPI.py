# -*- coding: utf-8 -*-
"""
Created on Fri Jun 16 10:55:25 2017

@author: Susie
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Mar  8 18:02:07 2017

@author: Susie
"""

account_name='blobsensordata'
account_key= '####'

from azure.storage.blob import BlockBlobService
import pandas as pd
from io import StringIO
from datetime import date, timedelta
from datetime import datetime
from decimal import Decimal

#from azure.storage.blob import PublicAccess
blob_service = BlockBlobService(account_name=account_name, account_key = account_key)

#----------------------------------------about adventis data---------------------------------------------------------#
# for HDB result
container_name = 'hdbwakeupsleeptime'
today = date.today()  
today = today.strftime('%Y-%m-%d')
Yst = date.today() - timedelta(1)
Yst = Yst.strftime('%Y-%m-%d')

blobs = []
generator = blob_service.list_blobs(container_name)
for blob in generator:
    blobs.append(blob.name)
print(len(blobs))
for blob in blobs:
    if blob==today:
        blob_string = blob_service.get_blob_to_text(container_name=container_name, blob_name=today)
        blob_class = blob_string.content
        blob_unicode= pd.read_csv(StringIO(blob_class),low_memory=False)
        blob_unicode = blob_unicode[blob_unicode['sleeptime']!="['nan']"]
        blob_unicode = blob_unicode[blob_unicode['wakeupTime']!="['nan']"]        
        blob_frame = blob_unicode[['hubid','wakeupTime','sleeptime']]
        blob_frame = blob_frame.rename(index=str,columns={'wakeupTime':"wakeup",'sleeptime':"sleep"})
    else:
        blob_frame = pd.DataFrame(columns = ['hubid','sleep','wakeup'])

out = blob_frame.to_json(orient='records')[1:-1]
out = out.replace('},{' , '}},{{')
out = out.split('},{')

import requests

test = []
for obj in out:
    test.append(obj.split(","))
    

import json
yalmfile = []
for strings in out:
    yalmfile.append(json.loads(strings))
#api = 'https://colife-dashboard.silverline.mobi/updateSleepInfo'
#r = requests.post(api, json = yalmfile)

api = 'https://portal-staging.silverline.mobi/updateSleepInfo'
r = requests.post(api, json = yalmfile)
