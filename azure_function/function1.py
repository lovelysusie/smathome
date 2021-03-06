#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep  6 11:26:38 2017

@author: Susie
"""
'''
This part is going to get the rms data and PIR data of today and put them in exact folder.
'''

import sys, os.path
import os
import json
import time
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'myvenv/Lib/site-packages')))

import pandas as pd
from datetime import datetime
from datetime import timedelta
from datetime import date
import pytz
from azure.storage.blob import BlockBlobService
from io import StringIO
from azure.storage.blob import AppendBlobService

day_now=0
day_before=1

account_name='####'
account_key='####'

blob_service = BlockBlobService(account_name=account_name, account_key = account_key)

def from_blob_load_data(account_name_,account_key_,container_name_,types):
    
    blobs = [];blob_date = []
    generator = blob_service.list_blobs(container_name_)
    for blob in generator:
        blobs.append(blob.name)
        blob_date.append(blob.name[:10])
    blob_table = pd.DataFrame()
    blob_table['date'] = blob_date
    blob_table['blobname'] = blobs    

    Today = date.today().strftime('%Y-%m-%d')
    Yst = (date.today() - timedelta(1)).strftime('%Y-%m-%d')
    blob_table = blob_table[(blob_table['date']==Today)|(blob_table['date']==Yst)]    

    if blob_table.shape[0]>0:
        blob_df = pd.DataFrame()
        for blobname in blob_table['blobname']:
            blob_Class = blob_service.get_blob_to_text(container_name=container_name_, blob_name = blobname)
            blob_String =blob_Class.content
            blob_df1 = pd.read_csv(StringIO(blob_String),low_memory=False)
            blob_df = blob_df.append(blob_df1)        
        
        blob_df.index = range(blob_df.shape[0])
        print(blob_df.shape[0])
        if types=='PIR':
            blob_df = blob_df#[blob_df['tasklocation']=='Bathroom']
    else:
        blob_df = pd.DataFrame()
    return blob_df


def add_hubid2rmsdata(rms_raw):
    
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7ae74' ),'hubid' ] = 'SG-04-testingN'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af6a' ),'hubid' ] = 'SG-04-hub00016'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af1d' ),'hubid' ] = 'SG-04-hub00018'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af1b' ),'hubid' ] = 'SG-04-testingQ'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af25' ),'hubid' ] = 'SG-04-inter001'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7adae' ),'hubid' ] = 'SG-04-testingK'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af42' ),'hubid' ] = 'SG-04-hub00001'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af7d' ),'hubid' ] = 'SG-04-hub00004'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af70' ),'hubid' ] = 'SG-04-hub00005'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af68' ),'hubid' ] = 'SG-04-hub00013'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af5d' ),'hubid' ] = 'SG-04-hub00010'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af4e' ),'hubid' ] = 'SG-04-hub00009'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af4a' ),'hubid' ] = 'SG-04-hub00006'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af7b' ),'hubid' ] = 'SG-04-hub00015'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af64' ),'hubid' ] = 'SG-04-hub00002'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af61' ),'hubid' ] = 'SG-04-hub00012'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af5b' ),'hubid' ] = 'SG-04-hub00008'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af2a' ),'hubid' ] = 'SG-04-hub00007'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af21' ),'hubid' ] = 'SG-04-hub00014'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af1f' ),'hubid' ] = 'SG-04-interdev'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af60' ),'hubid' ] = 'SG-04-starhub7'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af1e' ),'hubid' ] = 'SG-04-hub00019'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af6f' ),'hubid' ] = 'SG-04-hub00020'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af5e' ),'hubid' ] = 'SG-04-hub00021'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af47' ),'hubid' ] = 'SG-04-hub00017'

    return rms_raw

def time_normer(rawdata):
    rawdata = rawdata.dropna(subset=['starttime'],how='all')
    rawdata['time'] = rawdata['starttime'].apply(lambda x: str(x))
    rawdata['time'] = rawdata['time'].apply(lambda x: x[:19])
    rawdata['time'] = rawdata['time'].apply(lambda x: datetime.strptime(x,'%Y-%m-%dT%H:%M:%S')+timedelta(hours=8))
    rawdata['eventtime'] = rawdata['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')) 
    flag3 = setflag("09:00:01",day_now); flag4 = setflag("19:59:59",day_before)
    rawdata = rawdata[(rawdata['time']<flag3)&(rawdata['time']>flag4)]
    return rawdata

def setflag(timestamp,day):
    flag = date.today()- timedelta(day)
    flag = flag.strftime('%Y-%m-%d')
    flag = flag + ' '+ timestamp
    return datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")

container_name = 'rmsinput'
rms_whole = from_blob_load_data(account_name_ = account_name,account_key_=account_key,container_name_=container_name,types='rms')
rms_whole = rms_whole[['gwId','EventProcessedUtcTime']]
rms_whole = rms_whole.rename(index=str, columns={ 'EventProcessedUtcTime': "starttime"})
rms_whole = time_normer(rms_whole)
rms_whole = add_hubid2rmsdata(rms_whole)

append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)
#container_name = 'rmsinputclean'
#append_blob_service.create_container(container_name)
#Today = date.today().strftime('%Y-%m-%d')
append_blob_service.create_blob("function", "rms_raw_clean")
append_blob_service.append_blob_from_text("function", "rms_raw_clean", rms_whole.to_csv())


#--------------------second part PIR--------------------------------------
container_name = 'adventisdatainput'
blob_df_whole=from_blob_load_data(account_name,account_key,container_name,'PIR')
blob_df_whole = time_normer(blob_df_whole)
blob_df_whole = blob_df_whole[['deviceid','tasklocation','value','time']] 

append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)
# container_name = 'pirinputclean'
# append_blob_service.create_container(container_name)
# sleep_text = blob_df_whole.to_csv()
append_blob_service.create_blob("function", "pir_grouped")
append_blob_service.append_blob_from_text("function", "pir_grouped", blob_df_whole.to_csv())


def bathroom_table_prep(room_acttime_input):
    '''
    This function is going to get the bathroom raw data, which has not been filtered by the awake table
    NOTE: here delete 5 min for each tampstam so that when checking all room blank not getting confused
    '''
    bathroom_time = room_acttime_input[room_acttime_input['tasklocation']=='Bathroom']
    del bathroom_time['tasklocation']
    bathroom_time = bathroom_time.rename(index=str, columns={'value': "sum"})
    bathroom_time['gap'] = 0
    bathroom_time['time'] = bathroom_time['time'].apply(lambda x: x-timedelta(minutes = 5))
    return bathroom_time

def bathroom_checker(bathroom_input):
    # This function is going to eleminate the sunddently appear value 1 bathroom time stamp
    # bathroom_input  = bathroom_input.sort(['hub_id'], ascending = [1])
    if len(bathroom_input):
        bathroom_input['gap'] = bathroom_input['time'].diff() 
        bathroom_input['gap'][0] = timedelta(minutes = 5)
        bathroom_input['gap'] = bathroom_input['gap'].apply(lambda x: x.seconds)
        return bathroom_input[(bathroom_input['sum']!=1)|(bathroom_input['gap']==300)]
    else:
        pass

bathroom = bathroom_table_prep(blob_df_whole)
bathroom_table = pd.DataFrame()

hublist = [
    'SG-04-testingN',
    'SG-04-hub00016',
    'SG-04-develop4',
    'SG-04-testingQ',
    'SG-04-inter001',
    'SG-04-testingK',
    'SG-04-hub00001',
    'SG-04-hub00004',
    'SG-04-hub00005',
    'SG-04-hub00013',
    'SG-04-hub00006',
    'SG-04-hub00015',
    'SG-04-hub00002',
    'SG-04-hub00012',
    'SG-04-hub00008',
    'SG-04-hub00007',
    'SG-04-hub00014',
    'SG-04-interdev',
    'SG-04-starhub7',
    'SG-04-hub00019',
    'SG-04-hub00020',
    'SG-04-hub00021',
    'SG-04-hub00017',
    'SG-04-hub00010',
    'SG-04-hub00009'
]

for hub in hublist:
    bathroom_check = bathroom[bathroom['deviceid']==hub]
    bathroom_check = bathroom_checker(bathroom_check)
    bathroom_table = bathroom_table.append(bathroom_check)


append_blob_service.create_blob("function", "bathroom")
append_blob_service.append_blob_from_text("function", "bathroom", bathroom_table.to_csv())

'''
#--------------------third part RMS split--------------------------------------
container_name = 'rms'
blob_service_ = BlockBlobService(account_name=account_name, account_key = account_key)
blobs_ = 'hublist.csv'
blob_Class_ = blob_service_.get_blob_to_text(container_name=container_name, blob_name = blobs_)
blob_String_ =blob_Class_.content
hublist = pd.read_csv(StringIO(blob_String_),low_memory=False)


container_name = 'temp3'
append_blob_service.create_container(container_name)

for hub in hublist['HUB ID']:
    rms_data = rms_whole[rms_whole['hubid']==hub]
    sleep_text = rms_data.to_csv()
    append_blob_service.create_blob(container_name, hub)
    append_blob_service.append_blob_from_text(container_name, hub, sleep_text)
'''
