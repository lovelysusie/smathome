#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep  6 11:26:38 2017

@author: Susie
"""
import sys, os.path
import os
import json
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'myvenv/Lib/site-packages')))

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

def from_blob_load_data(account_name_,account_key_,container_name_,types):
    blob_service = BlockBlobService(account_name=account_name_, account_key = account_key_)
    blobs = [];blob_date = []
    generator = blob_service.list_blobs(container_name_)
    for blob in generator:
        blobs.append(blob.name)
        blob_date.append(blob.name[:10])
    blob_table = pd.DataFrame()
    blob_table['date'] = blob_date
    blob_table['blobname'] = blobs    

    Today = date.today(); Today = Today.strftime('%Y-%m-%d')
    Yst = date.today() - timedelta(1) ; Yst = Yst.strftime('%Y-%m-%d')
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
    flag3 = setflag("09:00:01",day_now,'normal'); flag4 = setflag("19:59:59",day_before,'normal')
    rawdata = rawdata[(rawdata['time']<flag3)&(rawdata['time']>flag4)]
    return rawdata

def setflag(timestamp,day,tz):
    flag = date.today()- timedelta(day)
    flag = flag.strftime('%Y-%m-%d')
    flag = flag + ' '+ timestamp
    flag = datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")
    if tz!='normal':
        flag = flag.replace(tzinfo=pytz.timezone('Asia/Singapore'))
    else:
        flag = flag
    return flag

account_name= '####'
account_key='####'

container_name = 'rmsinput'
rms_whole = from_blob_load_data(account_name_ = account_name,account_key_=account_key,container_name_=container_name,types='rms')
rms_whole = rms_whole[['gwId','EventProcessedUtcTime']]
rms_whole = rms_whole.rename(index=str, columns={ 'EventProcessedUtcTime': "starttime"})
rms_whole = time_normer(rms_whole)
rms_whole = add_hubid2rmsdata(rms_whole)

append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)
container_name = 'rmsinputclean'
append_blob_service.create_container(container_name)
sleep_text = rms_whole.to_csv()
Today = date.today(); Today = Today.strftime('%Y-%m-%d')
append_blob_service.create_blob(container_name, Today)
append_blob_service.append_blob_from_text(container_name, Today, sleep_text)


#--------------------second part PIR--------------------------------------
container_name = 'adventisdatainput'
blob_df_whole=from_blob_load_data(account_name,account_key,container_name,'PIR')
blob_df_whole = time_normer(blob_df_whole)

append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)
container_name = 'pirinputclean'
append_blob_service.create_container(container_name)
sleep_text = blob_df_whole.to_csv()
Today = date.today(); Today = Today.strftime('%Y-%m-%d')
append_blob_service.create_blob(container_name, Today)
append_blob_service.append_blob_from_text(container_name, Today, sleep_text)

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

