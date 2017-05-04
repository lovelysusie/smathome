# -*- coding: utf-8 -*-
"""
Created on Wed May  3 05:38:57 2017

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
mykey = '####'
table_service = TableService(account_name=myaccount, account_key=mykey)

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

def del_neighbor(data):
    data = data.drop_duplicates('time',keep='first')#.reset_index(drop=True)
    data.index = range(data.shape[0])
    data['gap'] = data['time'].diff()
    data['gap'].ix[0] = timedelta(seconds=1000)
    data['gap'] = data['gap'].apply(lambda x:x.seconds)
    data = data[data['gap']>600]
    return data

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

def time_picker(raw_wake_table,types):
    if types=='sleep':
        flag3 = setflag("03:00:00",0,'normal')
        sleep = raw_wake_table[raw_wake_table['time']<flag3]
        if sleep.shape[0]>1:
            sleep.index = range(sleep.shape[0])
            delta = sleep.at[1, 'time'] - sleep.at[0, 'time']
            if delta.seconds <2400:
                sleep = sleep.iloc[[1]]
            else :
                sleep = sleep.iloc[[0]]
            print(sleep)
            sleep = sleep.rename(index=str, columns={ 'time': 'sleep'})   
        if sleep.shape[0]==1:
            sleep['hubid'] = 'SG-04-avent001'    
            sleep = sleep.rename(index=str, columns={ 'time': 'sleep'})
            print(sleep)
        if sleep.shape[0]==0:
            print("hello, no data")
        return sleep
    if types =='wakeup':
        flag3 = setflag("05:00:00",0,'normal')
        wakeup = awake_table[awake_table['time']>flag3]
        if wakeup.shape[0]>1:
            wakeup.index = range(wakeup.shape[0])
            delta = wakeup.at[1, 'time'] - wakeup.at[0, 'time']
            if delta.seconds <2400:
                wakeup = wakeup.iloc[[0]]
            else :
                wakeup = wakeup.iloc[[1]]
            print(wakeup)
            wakeup['hubid'] = 'SG-04-avent001'
            wakeup = wakeup.rename(index=str, columns={ 'time': 'wakeup'})   
        if wakeup.shape[0]==1:
            wakeup['hubid'] = 'SG-04-avent001'    
            wakeup = wakeup.rename(index=str, columns={ 'time': 'wakeup'})
            print(wakeup)
        if wakeup.shape[0]==0:
            print("hello, no data")
        return wakeup
def get_grouped(rawdata,types):
    if types=='bathroom':
        rawdata['time'] = rawdata['starttime'].apply(lambda x: datetime.strptime(x,'%Y-%m-%dT%H:%M:%S.0000000Z')+timedelta(hours=8))
        rawdata['eventtime'] = rawdata['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')) 
        rawdata = rawdata.set_index(rawdata['eventtime'].map(parser.parse))
        bathroom_time = rawdata.groupby(pd.TimeGrouper('600s')).max()
        bathroom_time = bathroom_time.dropna(subset=['value'], how='all')
        bathroom_time['eventtime'] = bathroom_time.index
        bathroom_time.index = range(bathroom_time.shape[0])
        bathroom_time = bathroom_time[['time']]
        bathroom_time['1min'] = 0;bathroom_time['sum'] = 0;bathroom_time['5min'] = 0
        return bathroom_time


flag1_str = setflag('12:29:59',1,'s') #should be 20:29
flag1_str = flag1_str.strftime('%Y-%m-%dT%H:%M:%S')

flag2_str = setflag('00:30:01',0,'s') #should be 20:29
flag2_str = flag2_str.strftime('%Y-%m-%dT%H:%M:%S')

table_filter ="devId eq 'e47fb2f7ae63' and dataType eq '20' and eventprocessedutctime ge datetime" + "'"+flag1_str + ".000Z'" +"and eventprocessedutctime lt datetime'"+flag2_str+".000Z'"

tasks = table_service.query_entities('sensor', filter=table_filter)
rms_list = []
for task in tasks:
    testing = task.Timestamp
    rms_list.append(testing)
rms_data = pd.DataFrame()
rms_data['UTCtime'] = rms_list

rms_data = rms_data.sort('UTCtime', ascending=0)
rms_data['SGtime'] = rms_data['UTCtime'].apply(lambda x: x+timedelta(hours = 8))
rms_data['time'] = rms_data['SGtime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

rms_data = rms_data.set_index(rms_data['time'].map(parser.parse))
if rms_data.shape[0]!=0:
    one_time_table = rms_data.groupby(pd.TimeGrouper('60s')).size()    
    one_time_table,outliers = remove_outliers(one_time_table)
    one_time_table.max()    
    time_table = one_time_table.to_frame()
    time_table = time_table.rename(index=str, columns={ 0: "1min"})
    time_table['sum'] = pd.rolling_sum(time_table['1min'],5)
    time_table.loc[(time_table['sum']<12),2 ]= 0
    time_table.loc[(time_table['sum']>12),2 ]= time_table['sum']
    time_table = time_table.rename(index=str, columns={ 2: "5min"})
    time_table = time_table.set_index(time_table.index.map(parser.parse))
    print(len(time_table))
    awake_table = time_table.groupby(pd.TimeGrouper('600s')).max()
    awake_table = awake_table[awake_table['5min']>0]
else:
    awake_table = pd.DataFrame()
print(awake_table)
#----------------------------------from blob import Bathroom Data---------------------------------------------------------
account_name='blobsensordata'
account_key='####'
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
else:
    blob_df = pd.DataFrame()
#---------------------------------------------------------if there is bathroom data--------------------------------------------
if blob_df.shape[0]!=0:
    blob_Class = None;blob_String = None;blob_date = None;blob_df1 = None
    bathroom_time = get_grouped(blob_df,'bathroom')

    #----------add RMS data--------------------------------------------------
    if awake_table.shape[0]>0:
        awake_table['time'] = awake_table.index
        awake_table.index = range(awake_table.shape[0])
        flag = awake_table['time'];flag.index = range(flag.shape[0])
        flag1 = flag[0];k = len(flag)-1;flag2 = flag[k]

        bathroom_time = bathroom_time[(bathroom_time['time']>flag1) & (bathroom_time['time']<flag2)]
        awake_table = awake_table.append(bathroom_time)
        awake_table = del_neighbor(awake_table)
        #-------use HV table filter the bathroom data
        print(bathroom_time.ix[:0]['time'])
        print(flag1)
        print(awake_table)
        
        #-------------deal with HV talbe & select the wake up time--------------------------
        awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
        wakeup = time_picker(awake_table,'wakeup')
        #-------------deal with HV talbe & select the sleep time--------------------------
        sleep = time_picker(awake_table,'sleep')

    else:
        sleep = pd.DataFrame(); sleep['sleep'] = 'nan'
        wakeup = pd.DataFrame(); wakeup['wakeup'] = 'nan'
        
#---------------------------------------------------------if do not have bathroom data--------------------------------------------
if blob_df.shape[0]==0:
    if awake_table.shape[0]>0:
        awake_table['time'] = awake_table.index
        awake_table = del_neighbor(awake_table)
        #-------------deal with HV talbe & select the wake up time--------------------------
        print(len(awake_table))
        awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
        #--------------------deal with sleep time based on HV table------------------------------------
        wakeup = time_picker(awake_table,'wakeup')
        #-------------deal with HV talbe & select the sleep time--------------------------
        sleep = time_picker(awake_table,'sleep')
    else:
        sleep = pd.DataFrame(); sleep['sleep'] = 'nan'
        wakeup = pd.DataFrame(); wakeup['wakeup'] = 'nan'
#-----------------------------------upload to blob----------------------------------------------------------------------
account_name='blobsensordata'
account_key='####'

append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)

# The same containers can hold all types of blobs
container_name = 'rmsdata'
append_blob_service.create_container(container_name)
finaltable = pd.DataFrame()
finaltable['hubid'] = ['SG-04-avent001']; finaltable['sleep'] = sleep.ix[0]['sleep']; finaltable['wakeup'] = wakeup.ix[0]['wakeup'] 
sleep_text = finaltable.to_csv()
append_blob_service.create_blob(container_name, Today)
append_blob_service.append_blob_from_text(container_name, Today, sleep_text)
