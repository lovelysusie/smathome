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

blob_Class2 = blob_service.get_blob_to_text(container_name=container_name, blob_name = blobs[TodayNo].name)
blob_string2 = blob_Class2.content
blob_df2 = pd.read_csv(StringIO(blob_string2),low_memory=False)

blob_df = blob_df2.append(blob_df1)
print(blob_df1.shape[0])
print(blob_df2.shape[0])
print(blob_df.shape[0])

from datetime import datetime
blob_df['eventtime'] = datetime.strptime(blob_df['starttime'], "%Y-%m-%dT%H:%M:%S.%fZ")    

timeseries = []
for time in blob_df['starttime']:
     timeseries.append(datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.0000000Z"))
blob_df['eventtime'] = timeseries
print(blob_df.head(5))

blob_df = blob_df[blob_df['tasklocation']=='Bedroom']
hublist = blob_df.deviceid.unique()
hublist = list(hublist)

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
        print "hello world"        
    i=i+1
sleep_time = pd.DataFrame()
sleep_time['hubid'] = hublist 

starting = blob_hub1[blob_hub1['status1']=='start']
ending = blob_hub1[blob_hub1['status2']=='end']

flag = ''
sleep = []
    
    


#convert-time
#import dateutil.parser
#yourdate = dateutil.parser.parse("2007-03-04T21:08:12")
# the below part is for upload
#from azure.storage.blob import AppendBlobService
#import os
#print(os.getcwd())

#append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)

# The same containers can hold all types of blobs
#append_blob_service.create_container(container_name)

# Append blobs must be created before they are appended to

#append_blob_service.create_blob(container_name, 'myappendblob')
#append_blob_service.append_blob_from_text(container_name, 'myappendblob', u'Hello, world!')

#append_blob = append_blob_service.get_blob_to_text(container_name, 'myappendblob')





