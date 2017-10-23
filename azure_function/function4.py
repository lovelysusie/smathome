import sys, os.path
import os
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'myvenv/Lib/site-packages')))


import pandas as pd
from datetime import date
from azure.storage.blob import BlockBlobService
from io import StringIO
from azure.storage.blob import AppendBlobService
from dateutil import parser
from datetime import datetime
from datetime import timedelta




account_name='####'
account_key='####'


append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)
#append_blob_service.create_container('temp2')
#append_blob_service.create_container('temp4')
hublist = [
    'SG-04-testingN',
    'SG-04-hub00016',
    'SG-04-develop4',
    'SG-04-testingQ',
    'SG-04-inter001',
    'SG-04-testingK',
    'SG-04-hub00001',
    'SG-04-hub00004',
    'SG-04-hub00005',
    'SG-04-hub00013',
    'SG-04-hub00006',
    'SG-04-hub00015',
    'SG-04-hub00002',
    'SG-04-hub00012',
    'SG-04-hub00008',
    'SG-04-hub00007',
    'SG-04-hub00014',
    'SG-04-interdev',
    'SG-04-starhub7',
    'SG-04-hub00019',
    'SG-04-hub00020',
    'SG-04-hub00021',
    'SG-04-hub00017',
    'SG-04-hub00010',
    'SG-04-hub00009'
]

Today = date.today().strftime('%Y-%m-%d')
blob_service_ = BlockBlobService(account_name=account_name, account_key = account_key)

def data_from_blob(container_name_input,blob_name,types='main'):
    blob_Class_ = blob_service_.get_blob_to_text(container_name=container_name_input, blob_name = blob_name)
    blob_String_ =blob_Class_.content
    blob_df_whole = pd.read_csv(StringIO(blob_String_),low_memory=False)
    if (types == 'main')&(len(blob_df_whole)>0):
        blob_df_whole['time'] = blob_df_whole['time'].apply(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S")) 
    return blob_df_whole

def del_neighbor(data,types):
    if len(data)>0:
        data = data.drop_duplicates('time',keep='first')#.reset_index(drop=True)
        data = data.sort_values(['time'], ascending=[1])
        data.index = range(data.shape[0])       
        if types == 'wakeup':
            data['gap'] = data['time'].diff()
            data['gap'].ix[0] = timedelta(seconds=1000)
            data = data[data['gap']>timedelta(seconds=300)]
        if types == 'sleep':
            test = data['time'].diff().tolist()
            test.pop(0)
            test.append(timedelta(seconds=300))        
            data['gap'] = test
            data = data[data['gap']>timedelta(seconds=300)]
    return data

bathroom_whole = data_from_blob('temp2', Today+"_bathroom_whole")
awake_table_whole1 = pd.DataFrame()
awake_table_whole2 = pd.DataFrame()

for hub_id in hublist:
    bathroom_table = bathroom_whole[bathroom_whole['hub_id']==hub_id]
    # del bathroom_table['hub_id']
    awake_table = data_from_blob('temp1', hub_id)
    awake_table['hub_id'] = hub_id
    if bathroom_table.shape[0]!=0:             
        awake_table = awake_table.append(bathroom_table)
    print(len(awake_table))
    awake_table1 = del_neighbor(awake_table, 'wakeup') 
    awake_table_whole1 = awake_table_whole1.append(awake_table1)
    
    awake_table2 = del_neighbor(awake_table, 'sleep') 
    awake_table_whole2 = awake_table_whole2.append(awake_table2)
    
    #awake_table = awake_table.to_csv()

append_blob_service.create_blob('temp4', "awake_table_wakeup")
awake_table_whole1['gap'] = awake_table_whole1['gap'].apply(lambda x: x.seconds) 

append_blob_service.append_blob_from_text('temp4', "awake_table_wakeup", awake_table_whole1.to_csv())

append_blob_service.create_blob('temp4', "awake_table_sleep")
append_blob_service.append_blob_from_text('temp4', "awake_table_sleep", awake_table_whole2.to_csv())
awake_table_whole2['gap'] = awake_table_whole2['gap'].apply(lambda x: x.seconds) 
#print(awake_table_whole.head())
#print(bathroom_table.head())
