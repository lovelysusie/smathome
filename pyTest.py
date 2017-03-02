#account_name='blobsensordata'
#account_key='zUYv9mIC9KPr/k+Sa15y4mN6mtozuJcF/n979cqojT4HaMUj3ahEHaPBVtpDihwfO78JTk8sQ29xCaxGWfjtSA=='
#block_blob_service = azb.BlobService(account_name=account_name, account_key=account_key)
account_name='parkinsonblob'
account_key='xqbYRwVHQLogpIDeOridgxXzBdJQaA7OU6lRT8s8XkQjye3EPBJ7QFvJOQ/rlU5gDFE2OLaH5sg5BKzongYT8Q=='
#container_name = 'preprocessed-data'

from azure.storage.blob import BlockBlobService
from azure.storage.blob import PublicAccess
#from azure.storage.blob import PublicAccess
blob_service = BlockBlobService(account_name=account_name, account_key = account_key)
#blob_service.get_blob_to_path("rspark","blobname","localfilename")
blob_service.create_container('mycontainer', public_access=PublicAccess.Container)

print("hello world")
