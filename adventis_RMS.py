# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 12:05:38 2017

@author: Susie
"""

import pandas as pd
from datetime import datetime
from datetime import timedelta
from dateutil import parser
from datetime import date, timedelta
import pytz
from pytz import timezone
from azure.storage.blob import BlockBlobService
from io import StringIO
from decimal import Decimal
from azure.storage.table import TableService, Entity
from azure.storage.blob import AppendBlobService


myaccount = 'rmscolife'
mykey = 'aJhqu2VbzauSxzM5aK9JikRGYqlpBnLAM7tUQnuWletPjhRt98OHAGBe2LWmdVUVZD2AnfqkFbGYdNQlNHnYmQ=='
table_service = TableService(account_name=myaccount, account_key=mykey)

tasks = table_service.query_entities('sensor', filter="devId eq 'e47fb2f7ae63'and PartitionKey eq '20'")

def setflag(timestamp,day):
    flag = date.today()- timedelta(day)
    flag = flag.strftime('%Y-%m-%d')
    flag = flag + ' '+ timestamp
    flag = datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")
    flag = flag.replace(tzinfo=pytz.timezone('Asia/Singapore'))
    return flag
flag1 = setflag('20:59:59',1)
flag2 = setflag('08:30:01',0)

rms_list = []
for task in tasks:
    if (task.Timestamp >flag1) & (task.Timestamp <flag2):
        print(task.Timestamp)
        #task = Entity()
        #print(type(task))        
        testing = task.Timestamp
        rms_list.append(testing)
        #print(getValues(task))
rms_data = pd.DataFrame()
rms_data['UTCtime'] = rms_list

rms_data = rms_data.sort('UTCtime', ascending=0)
rms_data['SGtime'] = rms_data['UTCtime'].apply(lambda x: x+timedelta(hours = 8))
rms_data['time'] = rms_data['SGtime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

rms_data = rms_data.set_index(rms_data['time'].map(parser.parse))
if rms_data.shape[0]!=0:
    one_time_table = rms_data.groupby(pd.TimeGrouper('60s')).size()    

    def remove_outliers(table):
        outliers = table.to_frame()
        outliers['time'] = outliers.index
        outliers = outliers[outliers[0]>11]
        outliers.index = range(outliers.shape[0])            
        i=0    
        while i<table.shape[0]:
            if table[i]>11:
                table[i]=max(table[i-1],table[i+1])
            i = i+1
        return table, outliers    

    one_time_table,outliers = remove_outliers(one_time_table)
    one_time_table.max()    

    time_table = one_time_table.to_frame()
    time_table = time_table.rename(index=str, columns={ 0: "1min"})
    time_table['sum'] = pd.rolling_sum(time_table['1min'],5)
    time_table.loc[(time_table['sum']<12),2 ]= 0
    time_table.loc[(time_table['sum']>12),2 ]= time_table['sum']
    time_table = time_table.rename(index=str, columns={ 2: "5min"})
    time_table = time_table.set_index(time_table.index.map(parser.parse))
    awake_table = time_table.groupby(pd.TimeGrouper('600s')).max()
    #awake_table = awake_table[awake_table['5min']>0]
else:
    awake_table = pd.DataFrame()

#----------------------------------from blob import Bathroom Data---------------------------------------------------------
account_name='blobsensordata'
account_key='zUYv9mIC9KPr/k+Sa15y4mN6mtozuJcF/n979cqojT4HaMUj3ahEHaPBVtpDihwfO78JTk8sQ29xCaxGWfjtSA=='
container_name = 'adventisdatainput'

blob_service = BlockBlobService(account_name=account_name, account_key = account_key)

blobs = [];blob_date = []
generator = blob_service.list_blobs(container_name)
for blob in generator:
    blobs.append(blob.name)
    blob_date.append(blob.name[:10])

blob_table = pd.DataFrame()
blob_table['date'] = blob_date
blob_table['blobname'] = blobs

Today = date.today(); Today = Today.strftime('%Y-%m-%d')
Yst = date.today() - timedelta(1) ; Yst = Yst.strftime('%Y-%m-%d')
blob_table = blob_table[(blob_table['date']==Today)|(blob_table['date']==Yst)]

def merge_data(bathroom_data,RMS_data):
    bathroom_data['timestamp'] = bathroom_data['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    #clinical_data['clincal_readable_time'] = clinical_data['clincal_readable_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    #left join sensor and clincal data
    merged_data = bathroom_data.merge(RMS_data, how='left', left_on=bathroom_data['timestamp'],right_on=RMS_data['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')))
    #forward fill the remaining rows as the clincal data is at the freq of 10s where as sensor data is at the freq if 1s
    return merged_data

#---------------------------------------------------------if there is bathroom data--------------------------------------------
if blob_table.shape[0]>0:
    blob_df = pd.DataFrame()
    for blobname in blob_table['blobname']:
        blob_Class = blob_service.get_blob_to_text(container_name=container_name, blob_name = blobname)
        blob_String =blob_Class.content 
        blob_df1 = pd.read_csv(StringIO(blob_String),low_memory=False)
        blob_df = blob_df.append(blob_df1)    

    blob_df.index = range(blob_df.shape[0])
    print(blob_df.shape[0])
    blob_df = blob_df[(blob_df['deviceid']=='SG-04-avent001')&(blob_df['tasklocation']=='Bathroom')]  
    blob_df['time'] = blob_df['starttime'].apply(lambda x: datetime.strptime(x,'%Y-%m-%dT%H:%M:%S.0000000Z')+timedelta(hours=8))
    blob_df['eventtime'] = blob_df['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')) 
    blob_df = blob_df.set_index(blob_df['eventtime'].map(parser.parse))
    bathroom_time = blob_df.groupby(pd.TimeGrouper('600s')).max()
    bathroom_time = bathroom_time.dropna(subset=['value'], how='all')
    bathroom_time['eventtime'] = bathroom_time.index
    blob_Class = None;blob_String = None;blob_date = None;blob_df1 = None
    del bathroom_time['endtime']; del bathroom_time['name']; del bathroom_time['address']
    #----------add RMS data--------------------------------------------------
    if awake_table.shape[0]>0:
        awake_table['time'] = awake_table.index
        awake_table.index = range(awake_table.shape[0])
        #awake_table['time'] = awake_table['time'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
        awake_table['status1'] = 'duration'
        i=1;n = awake_table.shape[0]
        while (i < n):
            delta = awake_table.at[i, 'time'] - awake_table.at[i-1, 'time']
            if delta.seconds >601:
                awake_table.at[i-1, 'status1'] = 'start'    
            i=i+1
        awake_table.at[n-1,'status1'] = 'start'
        awake_table = awake_table[awake_table['status1']=='start']
        del awake_table['status1']
        print(awake_table)
        merged_data = merge_data(bathroom_time, awake_table)
        
        '''
        flag3 = setflag("13:00:00",1)#the real time is 05:00 26
        wakeup = awake_table[awake_table['time']>flag3]
        if wakeup.shape[0]!=0:
            wakeup.index = range(wakeup.shape[0])
            delta = wakeup.at[0, 'time'] - wakeup.at[1, 'time']
            if delta.seconds <2400:
                wakeup = wakeup.iloc[[0]]
            else :
                wakeup = wakeup.iloc[[1]]
            wakeup['hubid'] = 'SG-04-avent001'    
            wakeup = wakeup.rename(index=str, columns={ 'time': 'wakeup'})
            print(wakeup)
        else:
            print("hello, no data")
        '''
#---------------------------------------------------------if do not have bathroom data--------------------------------------------
if blob_table.shape[0]==0:
    if awake_table.shape[0]>0:
        awake_table['time'] = awake_table.index
        awake_table.index = range(awake_table.shape[0])
        #awake_table['time'] = awake_table['time'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
        awake_table['status1'] = 'duration'
        i=1;n = awake_table.shape[0]
        while (i < n):
            delta = awake_table.at[i, 'time'] - awake_table.at[i-1, 'time']
            if delta.seconds >601:
                awake_table.at[i-1, 'status1'] = 'start'    
            i=i+1
        awake_table.at[n-1,'status1'] = 'start'
        awake_table = awake_table[awake_table['status1']=='start']
        del awake_table['status1']
        print(awake_table)
        flag3 = setflag("13:00:00",1)#the real time is 05:00 26
        '''
        wakeup = awake_table[awake_table['time']>flag3]
        if wakeup.shape[0]!=0:
            wakeup.index = range(wakeup.shape[0])
            delta = wakeup.at[0, 'time'] - wakeup.at[1, 'time']
            if delta.seconds <2400:
                wakeup = wakeup.iloc[[0]]
            else :
                wakeup = wakeup.iloc[[1]]
            wakeup['hubid'] = 'SG-04-avent001'    
            wakeup = wakeup.rename(index=str, columns={ 'time': 'wakeup'})
            print(wakeup)
        else:
            print("hello, no bathroom and RMS data")
        '''
account_name='blobsensordata'
account_key='zUYv9mIC9KPr/k+Sa15y4mN6mtozuJcF/n979cqojT4HaMUj3ahEHaPBVtpDihwfO78JTk8sQ29xCaxGWfjtSA=='

append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)

# The same containers can hold all types of blobs
container_name = 'rmsdata'
append_blob_service.create_container(container_name)

sleep_text = awake_table.to_string()
append_blob_service.create_blob(container_name, 'wholeoutput0406')
append_blob_service.append_blob_from_text(container_name, 'wholeoutput0406', sleep_text)
merged_data = merged_data.to_string()
append_blob_service.create_blob(container_name, 'bathroom0406')
append_blob_service.append_blob_from_text(container_name, 'bathroom0406', merged_data)
