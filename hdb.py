# -*- coding: utf-8 -*-
"""
Created on Fri Mar  3 13:03:21 2017

@author: Susie
"""

account_name='blobsensordata'
account_key='####'

from azure.storage.blob import BlockBlobService
import pandas as pd
from io import StringIO
from datetime import date, timedelta
from datetime import datetime
from decimal import Decimal

#from azure.storage.blob import PublicAccess
blob_service = BlockBlobService(account_name=account_name, account_key = account_key)
#blob_service.get_blob_to_path("rspark","blobname","localfilename")
#blob_service.create_container('mycontainer', public_access=PublicAccess.Container)
container_name = 'hdbdatainput'

blobs = []
marker = None
while True:
    batch = blob_service.list_blobs(container_name, marker=marker)
    blobs.extend(batch)
    if not batch.next_marker:
        break
    marker = batch.next_marker
blob_list = []
blob_date = []
for blob in blobs:
    blob_list.append(blob.name)
    blob_date.append(blob.name[:10])

blob_table = pd.DataFrame()
blob_table['date'] = blob_date
blob_table['blobname'] = blob_list

Today = date.today()  
Today = Today.strftime('%Y-%m-%d')
Yst = date.today() - timedelta(1)
Yst = Yst.strftime('%Y-%m-%d')

blob_table = blob_table[(blob_table['date']==Today)|(blob_table['date']==Yst)]

blob_df = pd.DataFrame()
for blobname in blob_table['blobname']:
    blob_Class = blob_service.get_blob_to_text(container_name=container_name, blob_name = blobname)
    blob_String =blob_Class.content
    blob_df1 = pd.read_csv(StringIO(blob_String),low_memory=False)
    blob_df = blob_df.append(blob_df1)
blob_df.index = range(blob_df.shape[0])
print(blob_df.shape[0])

#convert-time


timeseries = []
for time in blob_df['starttime']:
     timeseries.append(datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.0000000Z")+ timedelta(hours=8))
     
blob_df['eventtime'] = timeseries
print(blob_df.head(5))

# blob_df = blob_df[(blob_df['deviceid']=='SG-04-avent001') | (blob_df['deviceid']=='SG-04-avent002')]

hublist = blob_df.deviceid.unique()
hublist = list(hublist)

blob_df = blob_df[blob_df['tasklocation']=='Bedroom']

blob_df['status1'] = 'duration'
blob_df['status2'] = 'duration'

sleep = []
wakeup = []
sleep_time = pd.DataFrame()
sleep_time['hubid'] = hublist 
        

for hdbid in hublist:
    blob_hub1 = blob_df[blob_df['deviceid']==hdbid]
    if blob_hub1.shape[0]==0:
        wakeup.append(Decimal('nan'))
        sleep.append(Decimal('nan'))
    else:
        blob_hub1.index = range(blob_hub1.shape[0])
        i = 1
        n = blob_hub1.shape[0]-1
        while (i < n):
           delta = blob_hub1.at[i, 'eventtime'] - blob_hub1.at[i-1, 'eventtime']
           if delta.seconds >1800:
               blob_hub1.at[i-1, 'status1'] = 'start'    
               blob_hub1.at[i, 'status2'] = 'end'    
           i=i+1
    
        starting = blob_hub1[blob_hub1['status1']=='start']
        ending = blob_hub1[blob_hub1['status2']=='end']

        flag = date.today() - timedelta(1)
        flag = flag.strftime('%Y-%m-%d')
        flag = flag + ' 20:29:59'
        flag = datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")

        starting = starting[starting['eventtime']>flag]
        if starting.shape[0]==0:
            k = blob_hub1.shape[0]-1
            sleep.append(blob_hub1.iloc[k]['eventtime'])
        else:
            sleep.append(starting.iloc[0]['eventtime'])

        # getting the wake up time

        starting = blob_hub1[blob_hub1['status1']=='start']
        ending = blob_hub1[blob_hub1['status2']=='end']

        flag = date.today()
        flag = flag.strftime('%Y-%m-%d')
        flag = flag + ' 08:30:01'
        flag = datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")

        ending = ending[ending['eventtime']<flag]
        
        flag = date.today()
        flag = flag.strftime('%Y-%m-%d')
        flag = flag + ' 05:00:01'
        flag = datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")

        if ending.shape[0]==0:
            blob_hub1 = blob_hub1[blob_hub1['eventtime']>date.today()]
            if blob_hub1.shape[0]==0:
                wakeupPoint = Decimal('nan')
            else:
                wakeupPoint = blob_hub1.iloc[0]['eventtime']
                if wakeupPoint <flag:
                    blob_hub1 = blob_hub1[blob_hub1['eventtime']>flag]
                    if blob_hub1.shape[0]==0:
                        wakeupPoint = Decimal('nan')
                    else:
                        wakeupPoint = blob_hub1.iloc[0]['eventtime']
       
        else:
            k = ending.shape[0]-1
            wakeupPoint = ending.iloc[k]['eventtime']
            if wakeupPoint <flag:
                blob_hub1 = blob_hub1[blob_hub1['eventtime']>flag]
                if blob_hub1.shape[0]==0:
                    wakeupPoint = Decimal('nan')
                else:
                    wakeupPoint = blob_hub1.iloc[0]['eventtime']
                
        wakeup.append(wakeupPoint)




sleep_time['sleeptime']=sleep
sleep_time['wakeupTime']=wakeup
sleep_time = sleep_time.dropna()


# the below part is for upload
from azure.storage.blob import AppendBlobService
import os
print(os.getcwd())

append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)

# The same containers can hold all types of blobs
container_name = 'hdbwakeupsleeptime'
append_blob_service.create_container(container_name)

# Append blobs must be created before they are appended to
sleep_text = sleep_time.to_string()
append_blob_service.create_blob(container_name, date.today().strftime('%Y-%m-%d'))
append_blob_service.append_blob_from_text(container_name, date.today().strftime('%Y-%m-%d'), sleep_text)

#append_blob = append_blob_service.get_blob_to_text(container_name, 'myappendblob')
