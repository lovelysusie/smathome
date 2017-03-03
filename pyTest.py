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
container_name = 'adventis-input'
container_name = 'mycontainer'
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
blob_Class = blob_service.get_blob_to_text(container_name=container_name, blob_name = blobs[TodayNo].name)
blob_string = blob_Class.content
blob_df = pd.read_csv(StringIO(blob_string),low_memory=False)
print(blob_df.head(5))

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





