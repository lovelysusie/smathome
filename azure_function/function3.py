#import sys, os.path
#import os
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'myvenv/Lib/site-packages')))

import pandas as pd
from datetime import date
from azure.storage.blob import BlockBlobService
from io import StringIO
from azure.storage.blob import AppendBlobService
from dateutil import parser
from datetime import datetime
from datetime import timedelta

def get_grouped(rawdata):
    rawdata = rawdata.set_index(rawdata['eventtime'].map(parser.parse))
    room_frame = rawdata.groupby([pd.TimeGrouper('300s'),'deviceid','tasklocation']).max()
    room_frame['index1'] = room_frame.index#.apply(lambda x: list(x))
    room_frame['index1'] = room_frame['index1'].apply(lambda x: list(x))
    room_frame['time'] = room_frame['index1'].apply(lambda x: x[0])
    room_frame['hub_id'] = room_frame['index1'].apply(lambda x: x[1])
    room_frame['tasklocation'] = room_frame['index1'].apply(lambda x: x[2])
    room_frame = room_frame[['hub_id','time','tasklocation','value']]
    room_frame.index = range(len(room_frame))
    return room_frame
    

def data_from_blob(container_name_input,blob_name,types='main'):
    container_name = container_name_input
    blob_service_ = BlockBlobService(account_name=account_name, account_key = account_key)
    blobs_ = blob_name
    blob_Class_ = blob_service_.get_blob_to_text(container_name=container_name, blob_name = blobs_)
    blob_String_ =blob_Class_.content
    blob_df_whole = pd.read_csv(StringIO(blob_String_),low_memory=False)
    if types == 'main':
        blob_df_whole['time'] = blob_df_whole['time'].apply(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S")) 
    return blob_df_whole

def bathroon_checker(bathroom_input):
    # This function is going to eleminate the sunddently appear value 1 bathroom time stamp
    bathroom_input['gap'] = bathroom_input['time'].diff() 
    bathroom_input['gap'][0] = timedelta(minutes = 5)
    bathroom_input['gap'] = bathroom_input['gap'].apply(lambda x: x.seconds)
    return bathroom_input[(bathroom_input['sum']!=1)|(bathroom_input['gap']==300)]


def awake_table_bathroom(hub_id,room_acttime_input,blob_name):
    awake_table = data_from_blob('temp1', blob_name)
    bathroom_time = bathroom_table_prep(room_acttime_input)
    awake_table.append(bathroom_time)
    return awake_table


account_name='####'
account_key='####'

Today = date.today().strftime('%Y-%m-%d')

container_name = 'pirinputclean'
blob_df_whole = data_from_blob(container_name,Today)


append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)

room_acttime_whole = get_grouped(blob_df_whole)
append_blob_service.create_blob('temp2', Today+"_roomacttime")
append_blob_service.append_blob_from_text('temp2', Today+"_roomacttime", room_acttime_whole.to_csv())


def bathroom_checker(bathroom_input):
    # This function is going to eleminate the sunddently appear value 1 bathroom time stamp
    # bathroom_input  = bathroom_input.sort(['hub_id'], ascending = [1])
    if len(bathroom_input):
        bathroom_input['gap'] = bathroom_input['time'].diff() 
        bathroom_input['gap'][0] = timedelta(minutes = 5)
        bathroom_input['gap'] = bathroom_input['gap'].apply(lambda x: x.seconds)
        return bathroom_input[(bathroom_input['sum']!=1)|(bathroom_input['gap']==300)]
    else:
        pass

def bathroom_table_prep(room_acttime_input):
    '''
    This function is going to get the bathroom raw data, which has not been filtered by the awake table
    NOTE: here delete 5 min for each tampstam so that when checking all room blank not getting confused
    '''
    bathroom_time = room_acttime_input[room_acttime_input['tasklocation']=='Bathroom']
    del bathroom_time['tasklocation']
    bathroom_time = bathroom_time.rename(index=str, columns={'value': "sum"})
    bathroom_time['gap'] = 0
    bathroom_time['time'] = bathroom_time['time'].apply(lambda x: x-timedelta(minutes = 5))
    return bathroom_time


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

bathroom = bathroom_table_prep(room_acttime_whole)
bathroom_table = pd.DataFrame()

for hub in hublist:
    bathroom_check = bathroom[bathroom['hub_id']==hub]
    bathroom_check = bathroom_checker(bathroom_check)
    bathroom_table = bathroom_table.append(bathroom_check)


append_blob_service.create_blob('temp2', Today+"_bathroom_whole")
append_blob_service.append_blob_from_text('temp2', Today+"_bathroom_whole", bathroom_table.to_csv())



