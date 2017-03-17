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
'''
Today = date.today()  
Today = Today.strftime('%Y-%m-%d')
Yst = date.today() - timedelta(5)
Yst = Yst.strftime('%Y-%m-%d')

path = '/Users/Susie/Downloads/Sensor.typed.csv'
rms_data = pd.read_csv(path, low_memory = True)
key = rms_data.columns.values[5]

rms_data = rms_data.dropna(subset=['EventProcessedUtcTime'], how='all')
test = rms_data[key].astype('str')
rms_data[key] = rms_data[key].astype('str')

def converttime(data):
    if (data!='nan'):
        data = datetime.strptime(data, "%Y-%m-%dT%H:%M:%S.%fZ")+ timedelta(hours=8)
    return data

rms_data['eventtime'] = rms_data[key].apply(lambda x: converttime(x))
rms_data['time'] = rms_data['eventtime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
rms_data = rms_data.set_index(rms_data['time'].map(parser.parse))
grouped = rms_data.groupby(pd.TimeGrouper('300s'))
count = rms_data.groupby(pd.TimeGrouper('300s')).size()
count.max()
rms_data = rms_data[rms_data['eventtime']>Yst]
count = rms_data.groupby(pd.TimeGrouper('600s')).size()
'''
from azure.storage.table import TableService, Entity
myaccount = 'rmscolife'
mykey = '####'
table_service = TableService(account_name=myaccount, account_key=mykey)

tasks = table_service.query_entities('sensor', filter="devId eq 'e47fb2f7adb2'and PartitionKey eq '20'")

import pytz
from pytz import timezone

def setflag(timestamp,day):
    flag = date.today()- timedelta(day)
    flag = flag.strftime('%Y-%m-%d')
    flag = flag + ' '+ timestamp
    flag = datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")
    flag = flag.replace(tzinfo=pytz.timezone('Asia/Singapore'))
    return flag
flag1 = setflag('20:59:59',6)
flag2 = setflag('08:30:01',5)

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
grouped = rms_data.groupby(pd.TimeGrouper('600s'))
time_table = rms_data.groupby(pd.TimeGrouper('600s')).size()
time_table = time_table.to_frame(name='value')
time_table['time'] = time_table.index
time_table.index = range(time_table.shape[0])

i = 1
n = time_table.shape[0]-1
high_value = []
while (i < n):
    g = time_table.at[i, 'value']
    k = time_table.at[i-1, 'value']
    j = time_table.at[i+1, 'value']
    x = g+k
    y = g+j
    if g >15:
        high_value.append(time_table.at[i, 'time'])   
    if ((g >10)&(g <15))&((k >5)|(j >5)):
        high_value.append(time_table.at[i, 'time'])
    if ((g>7)&(g<11))&((y>15)|(x>15)):
        high_value.append(time_table.at[i, 'time'])
    i=i+1

sleep_frame = pd.DataFrame()
hubid = []; hubid.append('SG-04-testingN')
sleep_frame['hubid'] = hubid
sleep = [] ; sleep.append(high_value[0])
sleep_frame['sleep'] = sleep
k = high_value.__len__()-1
wakeup = []; wakeup.append(high_value[k])
sleep_frame['wakeup'] = wakeup

#high_value = high_value.to_frame(name = 'value')
#g = high_value.__len__()
#high_value = high_value[0].append(high_value[g])

from azure.storage.blob import AppendBlobService

account_name='blobsensordata'
account_key='####'

append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)

# The same containers can hold all types of blobs
container_name = 'rmsdata'
append_blob_service.create_container(container_name)

# Append blobs must be created before they are appended to
#timetable_text = time_table.to_string()
#append_blob_service.create_blob(container_name, date.today().strftime('%Y-%m-%d'))
#append_blob_service.append_blob_from_text(container_name, date.today().strftime('%Y-%m-%d'), timetable_text)

#rms_text = rms_data.to_string()
#append_blob_service.create_blob(container_name, 'wholedata')
#append_blob_service.append_blob_from_text(container_name, 'wholedata', rms_text)

sleep_text = sleep_frame.to_string()
append_blob_service.create_blob(container_name, '2017-03-12')
append_blob_service.append_blob_from_text(container_name, '2017-03-12', sleep_text)
