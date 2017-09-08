import sys, os.path
import os
import json
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'myvenv/Lib/site-packages')))

import pandas as pd
from datetime import date
import pytz
from azure.storage.blob import BlockBlobService
from io import StringIO
from azure.storage.blob import AppendBlobService
from dateutil import parser
import pandas as pd
from datetime import datetime
from datetime import timedelta
from datetime import date
import pytz

day_now=0
day_before=1

def setflag(timestamp,day,tz):
    flag = date.today()- timedelta(day)
    flag = flag.strftime('%Y-%m-%d')
    flag = flag + ' '+ timestamp
    flag = datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")
    return flag

def get_grouped(rawdata,types):
    rawdata = rawdata.set_index(rawdata['eventtime'].map(parser.parse))
    if types=='bathroom':
        room_list = rawdata['tasklocation'].unique().tolist()
        room_whole = pd.DataFrame()
        for room in room_list:
            room_frame = rawdata[rawdata['tasklocation']==room]
            room_frame = room_frame.groupby(pd.TimeGrouper('300s')).max()
            room_frame = room_frame[['value','tasklocation']]
            room_whole = room_whole.append(room_frame)
        room_whole = room_whole.dropna(subset=['value'], how='all')
        room_whole['time'] = room_whole.index
        room_whole.index = range(room_whole.shape[0])
        return room_whole
    

def data_from_blob(container_name_input,blob_name,types='main'):
    container_name = container_name_input
    blob_service_ = BlockBlobService(account_name=account_name, account_key = account_key)
    blobs_ = blob_name
    blob_Class_ = blob_service_.get_blob_to_text(container_name=container_name, blob_name = blobs_)
    blob_String_ =blob_Class_.content
    blob_df_whole = pd.read_csv(StringIO(blob_String_),low_memory=False)
    if types == 'main':
        blob_df_whole['time'] = blob_df_whole['time'].apply(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S")) 
    if types != 'main':
        blob_df_whole = blob_df_whole
    return blob_df_whole


account_name='blobsensordata'
account_key='zUYv9mIC9KPr/k+Sa15y4mN6mtozuJcF/n979cqojT4HaMUj3ahEHaPBVtpDihwfO78JTk8sQ29xCaxGWfjtSA=='

Today = date.today(); Today = Today.strftime('%Y-%m-%d')

container_name = 'pirinputclean'
blob_df_whole = data_from_blob(container_name,Today)

container_name = 'rms'
blobs_ = 'hublist.csv'
hublist = data_from_blob(container_name,blobs_,'hublist')

container_name = 'temp2'
append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)
append_blob_service.create_container(container_name)

for hub in hublist['HUB ID']:
    print(hub)
    hub_id = hub
    blob_df = blob_df_whole[blob_df_whole['deviceid']==hub_id]
    if blob_df.shape[0]!=0:     
        room_acttime = get_grouped(blob_df,'bathroom')
        print("multiroom user")
    pir = room_acttime.to_csv()
    append_blob_service.create_blob(container_name, hub_id)
    append_blob_service.append_blob_from_text(container_name, hub_id, pir)
