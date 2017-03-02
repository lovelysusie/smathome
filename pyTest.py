#account_name='parkinsonblob'
#account_key='xqbYRwVHQLogpIDeOridgxXzBdJQaA7OU6lRT8s8XkQjye3EPBJ7QFvJOQ/rlU5gDFE2OLaH5sg5BKzongYT8Q=='
#
account_name='blobsensordata'
account_key='zUYv9mIC9KPr/k+Sa15y4mN6mtozuJcF/n979cqojT4HaMUj3ahEHaPBVtpDihwfO78JTk8sQ29xCaxGWfjtSA=='
#container_name = 'preprocessed-data'
import time
from azure.storage.blob import BlockBlobService
from azure.storage.blob import PublicAccess
#from azure.storage.blob import PublicAccess
blob_service = BlockBlobService(account_name=account_name, account_key = account_key)
#blob_service.get_blob_to_path("rspark","blobname","localfilename")
blob_service.create_container('mycontainer', public_access=PublicAccess.Container)
todayDate = time.strftime("%Y-%m-%d")
print("hello world")
from datetime import datetime, timedelta

yesterday = datetime.now() - timedelta(days=1)
yesterday = yesterday.strftime("%Y-%m-%d")

container_name = 'adventis-input'
#container_name = 'preprocessed-data'
#blob_name ='2017-03-02/326454415_86a7973c64a7481784acfcd9578bd964_1.csv'
#blob_string = blob_service.get_blob_to_text(container_name=container_name, blob_name=blob_name)

containers = blob_service.list_blob(container_name=container_name)

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
blob_string = blob_service.get_blob_to_text(container_name=container_name, blob_name = blobs[0].name)
    
