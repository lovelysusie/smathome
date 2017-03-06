#account_name='parkinsonblob'
#account_key='xqbYRwVHQLogpIDeOridgxXzBdJQaA7OU6lRT8s8XkQjye3EPBJ7QFvJOQ/rlU5gDFE2OLaH5sg5BKzongYT8Q=='
#
account_name='blobsensordata'
account_key='zUYv9mIC9KPr/k+Sa15y4mN6mtozuJcF/n979cqojT4HaMUj3ahEHaPBVtpDihwfO78JTk8sQ29xCaxGWfjtSA=='
#container_name = 'preprocessed-data'
from azure.storage.blob import BlockBlobService
#from azure.storage.blob import PublicAccess
import pandas as pd
from io import StringIO
#from azure.storage.blob import PublicAccess
blob_service = BlockBlobService(account_name=account_name, account_key = account_key)
#blob_service.get_blob_to_path("rspark","blobname","localfilename")
#blob_service.create_container('mycontainer', public_access=PublicAccess.Container)
container_name = 'adventisdatainput'
#container_name = 'mycontainer'
#blob_name ='2017-03-02/326454415_86a7973c64a7481784acfcd9578bd964_1.csv'
#blob_string = blob_service.get_blob_to_text(container_name=container_name, blob_name=blob_name)

blobs = []
marker = None
while True:
    batch = blob_service.list_blobs(container_name, marker=marker)
    blobs.extend(batch)
    if not batch.next_marker:
        break
    marker = batch.next_marker
for blob in blobs:
    print(blob.name)
TodayNo = len(blobs)-1    
YstNo = len(blobs)-2
blob_Class1 = blob_service.get_blob_to_text(container_name=container_name, blob_name = blobs[TodayNo].name)
blob_string1 = blob_Class1.content
blob_df1 = pd.read_csv(StringIO(blob_string1),low_memory=False)

blob_Class2 = blob_service.get_blob_to_text(container_name=container_name, blob_name = blobs[YstNo].name)
blob_string2 = blob_Class2.content
blob_df2 = pd.read_csv(StringIO(blob_string2),low_memory=False)

blob_df = blob_df2.append(blob_df1)
print(blob_df1.shape[0])
print(blob_df2.shape[0])
print(blob_df.shape[0])

#convert-time
from datetime import datetime
from datetime import timedelta

timeseries = []
for time in blob_df['starttime']:
     timeseries.append(datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.0000000Z")+ timedelta(hours=8))
     
blob_df['eventtime'] = timeseries
print(blob_df.head(5))

blob_df = blob_df[blob_df['tasklocation']=='Bedroom']
blob_df = blob_df[(blob_df['deviceid']=='SG-04-avent001') | (blob_df['deviceid']=='SG-04-avent002')]

hublist = blob_df.deviceid.unique()
hublist = list(hublist)
print(hublist)

blob_df['status1'] = 'duration'
blob_df['status2'] = 'duration'

blob_hub1 = blob_df[blob_df['deviceid']==hublist[0]]
blob_hub1.index = range(blob_hub1.shape[0])

i = 1
n = blob_hub1.shape[0]-1
while (i < n):
    delta = blob_hub1.at[i, 'eventtime'] - blob_hub1.at[i-1, 'eventtime']
    if delta.seconds >1800:
        blob_hub1.at[i-1, 'status1'] = 'start'    
        blob_hub1.at[i, 'status2'] = 'end'    
    i=i+1
sleep_time = pd.DataFrame()
sleep_time['hubid'] = hublist 

starting = blob_hub1[blob_hub1['status1']=='start']
ending = blob_hub1[blob_hub1['status2']=='end']

from datetime import date, timedelta
flag = date.today() - timedelta(1)
flag = flag.strftime('%Y-%m-%d')
flag = flag + ' 20:29:59'
flag = datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")

starting = starting[starting['eventtime']>flag]

sleep = []
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

k = ending.shape[0]-1
wakeupPoint = ending.iloc[k]['eventtime']

wakeup = []
if ending.shape[0]==0:
    blob_hub1 = blob_hub1[blob_hub1['eventtime']>date.today()]
    wakeupPoint = blob_hub1.iloc[0]['eventtime']
if wakeupPoint <flag:
    blob_hub1 = blob_hub1[blob_hub1['eventtime']>flag]
    wakeupPoint = blob_hub1.iloc[0]['eventtime']
    
wakeup.append(wakeupPoint)

print(wakeup)
print(sleep)
print(hublist)

blob_hub2 = blob_df[blob_df['deviceid']==hublist[1]]
blob_hub2.index = range(blob_hub2.shape[0])

i = 1
n = blob_hub2.shape[0]-1
while (i < n):
    delta = blob_hub2.at[i, 'eventtime'] - blob_hub2.at[i-1, 'eventtime']
    if delta.seconds >1800:
        blob_hub2.at[i-1, 'status1'] = 'start'    
        blob_hub2.at[i, 'status2'] = 'end'    
    i=i+1

starting = blob_hub2[blob_hub2['status1']=='start']
ending = blob_hub2[blob_hub2['status2']=='end']

flag = date.today() - timedelta(1)
flag = flag.strftime('%Y-%m-%d')
flag = flag + ' 20:29:59'
flag = datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")

starting = starting[starting['eventtime']>flag]

sleep.append(starting.iloc[0]['eventtime'])

# getting the wake up time

starting = blob_hub2[blob_hub2['status1']=='start']
ending = blob_hub2[blob_hub2['status2']=='end']

flag = date.today()
flag = flag.strftime('%Y-%m-%d')
flag = flag + ' 08:30:01'
flag = datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")

ending = ending[ending['eventtime']<flag]

flag = date.today()
flag = flag.strftime('%Y-%m-%d')
flag = flag + ' 05:00:01'
flag = datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")

k = ending.shape[0]-1
wakeupPoint = ending.iloc[k]['eventtime']

if ending.shape[0]==0:
    blob_hub2 = blob_hub2[blob_hub2['eventtime']>date.today()]
    wakeupPoint = blob_hub2.iloc[0]['eventtime']
if wakeupPoint <flag:
    blob_hub2 = blob_hub1[blob_hub2['eventtime']>flag]
    wakeupPoint = blob_hub2.iloc[0]['eventtime']
    
wakeup.append(wakeupPoint)
sleep_time['sleeptime']=sleep
sleep_time['wakeupTime']=wakeup

# the below part is for upload
from azure.storage.blob import AppendBlobService

append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)

# The same containers can hold all types of blobs
container_name = 'sleepwakeuptime'
append_blob_service.create_container(container_name)

# Append blobs must be created before they are appended to
sleep_text = sleep_time.to_string()
append_blob_service.create_blob(container_name, date.today().strftime('%Y-%m-%d'))
append_blob_service.append_blob_from_text(container_name, date.today().strftime('%Y-%m-%d'), sleep_text)

#append_blob = append_blob_service.get_blob_to_text(container_name, 'myappendblob')


