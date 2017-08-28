# -*- coding: utf-8 -*-
"""
Created on Fri May  5 08:19:51 2017

@author: Susie
"""

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
from azure.storage.blob import BlobService
from io import StringIO
from decimal import Decimal
#from azure.storage.blob import AppendBlobService
import RMS_function as RMS

account_name = "####"
account_key = "####"
#----------------load hublist for looping---------------------------------------
container_name = 'rms'
blob_service_ = BlobService(account_name=account_name, account_key = account_key)
blobs_ = 'hublist.csv'
blob_Class_ = blob_service_.get_blob_to_text(container_name=container_name, blob_name = blobs_)
blob_String_ =blob_Class_
hublist = pd.read_csv(StringIO(blob_String_),low_memory=False)
#------------------------------------load the whole rms data with many device-----------------------------------------
container_name = 'rmsinput'
rms_whole = RMS.from_blob_load_data(account_name_ = account_name,account_key_=account_key,container_name_=container_name,types='rms')
rms_whole = rms_whole[['gwId','EventProcessedUtcTime']]
rms_whole = rms_whole.rename(index=str, columns={ 'EventProcessedUtcTime': "starttime"})
rms_whole = RMS.time_normer(rms_whole)
rms_whole = RMS.add_hubid2rmsdata(rms_whole)

#----------------------------------from blob import Bathroom Data---------------------------------------------------------
container_name = 'adventisdatainput'
blob_df_whole=RMS.from_blob_load_data(account_name,account_key,container_name,'PIR')
blob_df_whole = RMS.time_normer(blob_df_whole)


hublist['wakeup'] = hublist['HUB ID'].apply(lambda x: RMS.final_generator(x,'wakeup',rms_whole,blob_df_whole))
hublist['sleep'] = hublist['HUB ID'].apply(lambda x: RMS.final_generator(x,'sleep',rms_whole,blob_df_whole))

'''
#hublist['sleep'][3] = RMS.setflag("23:40:00",1,'normal')
#hublist['wakeup'][9] = RMS.setflag("05:45:00",0,'normal')

hub_id = 'SG-04-testingN'#wakeup
hub_id = 'SG-04-starhub7'
hub_id = 'SG-04-starhub8'
hub_id = 'SG-04-testingQ'
hub_id = 'SG-04-inter001'
hub_id = 'SG-04-testingK'
hub_id = 'SG-04-hub00001'
hub_id = 'SG-04-hub00004'
hub_id = 'SG-04-swaritAK'
hub_id = 'SG-04-aylaayla'
hub_id = 'SG-04-starhub6'
hub_id = 'SG-04-hub00005'
hub_id = 'SG-04-hub00013'#wakeup
hub_id = 'SG-04-hub00010'
hub_id = 'SG-04-hub00009'
hub_id = 'SG-04-hub00007'


RMS.final_generator(hub_id,'wakeup',rms_whole,blob_df_whole)
hublist.to_csv("/Users/Susie/Desktop/hublist2.csv")

rms_data = rms_whole[rms_whole['hubid']==hub_id]
blob_df = blob_df_whole[blob_df_whole['deviceid']==hub_id]
awake_table = RMS.get_grouped(rms_data,'bedroom')
if awake_table.shape[0]!=0:
    awake_table = RMS.check_awake_table(awake_table,rms_data)
#awake_table = awake_table[awake_table['sum']>12]
room_acttime = RMS.get_grouped(blob_df,'bathroom')
        
bathroom_time = RMS.bathroom_table_prep(room_acttime)
#bathroom_time['time'][2]

awake_table = awake_table.append(bathroom_time)
awake_table = RMS.del_neighbor(awake_table,'wakeup')
awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
RMS.time_picker(awake_table,'wakeup',bathroom_time,rms_data,room_acttime)

#-------------testing area---------------------
flag3 = RMS.setflag("05:00:00",day_now,'normal')
wakeup = awake_table[awake_table['time']>flag3]
wakeup = wakeup[wakeup['gap']!=600]        
wakeup.index = range(wakeup.shape[0])

check_frame = room_acttime[(room_acttime['time']>wakeup.at[0, 'time'])&(room_acttime['time']<wakeup.at[1, 'time'])]
rms_data_check = rms_data[(rms_data['time']>(wakeup.at[0,'time']+timedelta(minutes=5)))&(rms_data['time']<(wakeup.at[1,'time']-timedelta(minutes=5)))]
                
bathroom_check = bathroom_time[(bathroom_time['time']>=(wakeup.at[0,'time'])) & (bathroom_time['time']<(wakeup.at[1,'time']))]
                    

'''