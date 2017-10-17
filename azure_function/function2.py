import sys, os.path
import os
import json
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'myvenv/Lib/site-packages')))

import pandas as pd
from datetime import datetime
from datetime import timedelta
from datetime import date
from azure.storage.blob import BlockBlobService
from io import StringIO
from azure.storage.blob import AppendBlobService
from dateutil import parser

day_now=0
day_before=1

def setflag(timestamp,day):
    flag = date.today()- timedelta(day)
    flag = flag.strftime('%Y-%m-%d')
    flag = flag + ' '+ timestamp
    return datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")

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
    if types=='bedroom':
        if rawdata.shape[0]!=0:
            one_time_table = rawdata.groupby(pd.TimeGrouper('60s')).size() 
            five_time_table = rawdata.groupby(pd.TimeGrouper('300s')).size()    
            one_time_table = remove_outliers(one_time_table)
            time_table = one_time_table.to_frame()
            time_table = time_table.rename(index=str, columns={ 0: "1min"})
            time_table['sum'] = pd.rolling_sum(time_table['1min'],5)
            time_table.loc[(time_table['sum']<12),'sum']= 0
            time_table.loc[(time_table['sum']>12),'sum']= time_table['sum']
            #time_table = time_table.rename(index=str, columns={ 2: "5min"})
            time_table = time_table.set_index(time_table.index.map(parser.parse))
            awake_table = time_table.groupby(pd.TimeGrouper('300s')).max()
            awake_table['5min'] = five_time_table
            awake_table['time'] = awake_table.index
            awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
            awake_table['gap'] = awake_table['5min'].diff()
            awake_table = awake_table[(awake_table['sum']>12)|(awake_table['gap']<-5)]
            del awake_table['1min']; del awake_table['5min']
            awake_table.index = range(awake_table.shape[0])
        else:
            awake_table = pd.DataFrame()
        return awake_table

    
def check_awake_table(awake_table_input,raw_rms_input):
    '''
    This function is going to check whether the awake table is picking up all the morning and
     evening movement timestamp.
     if not, it will return timestamp following by the lone period blank
    '''
    flag2=setflag("05:00:00",day_now)
    awake_table_test1 = awake_table_input[awake_table_input['time']>flag2]

    flag1=setflag("02:00:00",day_now)
    awake_table_test2 = awake_table_input[awake_table_input['time']<flag1]

    rawdata = raw_rms_input.set_index(raw_rms_input['eventtime'].map(parser.parse))
    rawdata['eventtime'] = rawdata['eventtime'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))

    if (awake_table_test1.shape[0]==0)&(awake_table_test2.shape[0]!=0):#wake up have not been capture
        rawdata = rawdata[rawdata['eventtime']>flag2]

    if (awake_table_test1.shape[0]!=0)&(awake_table_test2.shape[0]==0):#sleep have not been capture
        rawdata = rawdata[rawdata['eventtime']<flag1]

    if (awake_table_test1.shape[0]!=0)&(awake_table_test2.shape[0]!=0):#both have not been capture
        rawdata = pd.DataFrame()
    if rawdata.shape[0]!=0:
        five_time_table = rawdata.groupby(pd.TimeGrouper('300s')).size().to_frame()
        five_time_table = five_time_table.rename(index=str, columns={ 0: 'sum'})
        five_time_table['time'] = five_time_table.index
        five_time_table['time'] = five_time_table['time'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
        five_time_table.index = range(five_time_table.shape[0])
        first_line = five_time_table[:1]
        blank_time = five_time_table[five_time_table['sum']==0]
        blank_time['gap'] = blank_time['time'].diff()
        blank_time.index = range(blank_time.shape[0])
        last_line = five_time_table[-1:]

        if blank_time.shape[0]>2:
            blank_time['gap'].ix[0] = blank_time['time'][0]-first_line['time'][0]
            blank_time['gap'] = blank_time['gap'].apply(lambda x: x.seconds)
            i=0; n=blank_time.shape[0]-1
            add_frame = pd.DataFrame()
            while i<n:
                if (blank_time['gap'][i]==300)&(blank_time['gap'][i+1]==300):
                    add_line = blank_time.loc[[i]]
                    add_frame = add_frame.append(add_line)
                i=i+1
            #add_frame['time'] = add_frame['time'].apply(lambda x: x-timedelta(seconds=300))
            add_frame = add_frame.append(last_line)    

        if blank_time.shape[0]==2:
            add_frame = blank_time.loc[[1]]
            add_frame['time'] = add_frame['time'].apply(lambda x: x-timedelta(seconds=300))
            add_frame = add_frame.append(last_line)    

        if blank_time.shape[0]==1:
            add_frame = blank_time.loc[[0]]
            add_frame['time'] = add_frame['time'].apply(lambda x: x-timedelta(seconds=300))
            add_frame = add_frame.append(last_line)    
        if blank_time.shape[0]==0:
            add_frame = five_time_table[-1:]
            
        add_frame['gap'] = 20;add_frame['sum'] = 20    
        awake_table_input = awake_table_input.append(add_frame)
    if rawdata.shape[0]==0:
        awake_table_input = awake_table_input
    return awake_table_input

def data_from_blob(container_name_input,blob_name,types='main'):
    container_name = container_name_input
    blob_service_ = BlockBlobService(account_name=account_name, account_key = account_key)
    blobs_ = blob_name
    blob_Class_ = blob_service_.get_blob_to_text(container_name=container_name, blob_name = blobs_)
    blob_String_ =blob_Class_.content
    blob_df_whole = pd.read_csv(StringIO(blob_String_),low_memory=False)
    return blob_df_whole

def remove_outliers(table):
    i=1 ;n = table.shape[0]-1    
    while i<n:
        if table[i]>11:
            table[i]=max(table[i-1],table[i+1])
        i = i+1
    return table    

account_name='####'
account_key='####'

Today = date.today().strftime('%Y-%m-%d')

container_name = 'rmsinputclean'
rms_whole = data_from_blob(container_name,Today)

container_name = 'rms'
blobs_ = 'hublist.csv'
hublist = data_from_blob(container_name,blobs_,'hublist')

container_name = 'temp1'
append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)
append_blob_service.create_container(container_name)

for hub in hublist['HUB ID']:
    print(hub)
    hub_id = hub
    rms_data = rms_whole[rms_whole['hubid']==hub_id]
    awake_table = get_grouped(rms_data,'bedroom')
    if awake_table.shape[0]!=0:
        awake_table = check_awake_table(awake_table,rms_data)
    rms = awake_table.to_csv()
    append_blob_service.create_blob(container_name, hub_id)
    append_blob_service.append_blob_from_text(container_name, hub_id, rms)
    





