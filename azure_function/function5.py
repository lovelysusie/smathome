import sys, os.path
import os
import json
import time
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'myvenv/Lib/site-packages')))

import pandas as pd
from datetime import datetime
from datetime import timedelta
from dateutil import parser
from datetime import date
from azure.storage.blob import BlockBlobService
from io import StringIO
day_now=0
day_before=1
account_name='####'
account_key='####'

blob_service_ = BlockBlobService(account_name=account_name, account_key = account_key)

def setflag(timestamp,day):
    flag = date.today()- timedelta(day)
    flag = flag.strftime('%Y-%m-%d')
    flag = flag + ' '+ timestamp
    return datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")

def get_agv_int(rms_data_input):
    rms_data_input['eventtime'] = rms_data_input['eventtime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))  
    intensity = rms_data_input.set_index('eventtime').groupby(pd.TimeGrouper(freq='5Min')).size()  
    intensity = intensity.to_frame()
    intensity = intensity[intensity[0]!=0]  
    output = intensity[0].mean()    
    return output

def bef_aft(wakeupinput,rms_data_input):
    wakeupinput.index = range(wakeupinput.shape[0])
    rms_data_check = rms_data_input[(rms_data_input['time']>(wakeupinput.at[0,'time']+timedelta(minutes=5)))&(rms_data_input['time']<(wakeupinput.at[1,'time']-timedelta(minutes=5)))]
    if rms_data_check.shape[0]>4:
        agv_int = get_agv_int(rms_data_check)
        if agv_int<=5:
            wakeupinput = wakeupinput.loc[[1]]
        else:
            wakeupinput = wakeupinput.loc[[0]]
    if rms_data_check.shape[0] <= 4:
        wakeupinput = wakeupinput.loc[[0]]
    return wakeupinput


def time_picker(raw_wake_table,types,rms_data_input,room_acttime_input):
    if types=='sleep':
        sleep = raw_wake_table[(raw_wake_table['time']<setflag("01:30:01",day_now))&(raw_wake_table['time']>setflag("20:59:59",day_before))]
        if sleep.shape[0]==2:
            if (sleep.at[sleep.index[1], 'time']-sleep.at[sleep.index[0], 'time']).seconds>=2400:
                sleep.at[sleep.index[0], 'time'] = sleep.at[sleep.index[0], 'time']+timedelta(minutes=10)
            #check_frame = room_acttime_input
            check_frame = room_acttime_input[(room_acttime_input['time']>sleep.at[sleep.index[0], 'time'])&(room_acttime_input['time']<sleep.at[sleep.index[1], 'time'])]
            if check_frame.shape[0]==0:
                sleep = sleep.loc[[sleep.index[0]]]
            if check_frame.shape[0]==1:
                if ((check_frame.at[check_frame.index[0],'time']-sleep.at[sleep.index[0],'time']).seconds<=300)|\
                ((sleep.at[sleep.index[1],'time']-check_frame.at[check_frame.index[0],'time']).seconds<=300):
                    sleep = sleep.loc[[sleep.index[0]]]
                else:
                    sleep = sleep.loc[[sleep.index[1]]]
            if check_frame.shape[0]>1:
                sleep = sleep.loc[[sleep.index[1]]]    
        if sleep.shape[0]>2:
            if (sleep.at[sleep.index[2], 'time']-sleep.at[sleep.index[1], 'time']).seconds>=2400:
                sleep.at[sleep.index[1], 'time'] = sleep.at[sleep.index[1], 'time']+timedelta(minutes=10)
            check_frame = room_acttime_input
            check_frame = room_acttime_input[(room_acttime_input['time']>sleep.at[sleep.index[1], 'time'])&(room_acttime_input['time']<sleep.at[sleep.index[2], 'time'])]
            if check_frame.shape[0]==0:
                sleep = sleep.loc[[sleep.index[1]]]
            if check_frame.shape[0]==1:
                if ((check_frame.at[check_frame.index[0],'time']-sleep.at[sleep.index[1],'time']).seconds<=300)|\
                ((sleep.at[sleep.index[2],'time']-check_frame.at[check_frame.index[0],'time']).seconds<=300):
                    sleep = sleep.loc[[sleep.index[1]]]
                else:
                    sleep = sleep.loc[[sleep.index[2]]]
            if check_frame.shape[0]>1:
                sleep = sleep.loc[[sleep.index[2]]]
        sleep = sleep.rename(index=str, columns={ 'time': 'sleep'})
        return sleep
    if types =='wakeup':
        #flag3 = setflag("05:00:00",day_now)
        wakeup = raw_wake_table[raw_wake_table['time']>setflag("05:00:00",day_now)]
        if wakeup.shape[0]==2:
        	bef_aft(wakeup,rms_data_input)
        if wakeup.shape[0]>2:
            wakeup.index = range(wakeup.shape[0])
            rms_data_check = rms_data_input[(rms_data_input['time']>(wakeup.at[0,'time']+timedelta(minutes=5)))&(rms_data_input['time']<(wakeup.at[1,'time']-timedelta(minutes=5)))]                
            if rms_data_check.shape[0]>4:
                agv_int = get_agv_int(rms_data_check)
                if agv_int<=5:
                    wakeup.drop(wakeup.index[[0]])
                    wakeup = bef_aft(wakeup,rms_data_input)
                if agv_int>5:
                    wakeup = wakeup.loc[[0]]
            if rms_data_check.shape[0] <= 4:
                wakeup = wakeup.loc[[0]]
        wakeup = wakeup.rename(index=str, columns={ 'time': 'wakeup'})   
        return wakeup




def get_data(containername,blobname):
    
    blob_Class_ = blob_service_.get_blob_to_text(container_name=containername, blob_name = blobname)
    blob_String_ =blob_Class_.content
    result = pd.read_csv(StringIO(blob_String_),low_memory=False) 
    return result

def final_generator(hub_id,types):
    awake_table_wakeup = get_data('temp4',"awake_table_wakeup")
    awake_table = awake_table_wakeup[awake_table_wakeup['hub_id']==hub_id] 
    rms_data = get_data('temp3', hub_id)
    room_acttime = pd.DataFrame()
    try:
        awake_table['time'] = awake_table['time'].apply(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))
        rms_data['time'] = rms_data['time'].apply(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))
    except:
        pass
    if len(awake_table)>0:
        awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
        wakeup = time_picker(awake_table,'wakeup',rms_data,room_acttime)
    else:
        pass
    if types=='wakeup':
        try:            
            return wakeup.ix[0]['wakeup']
        except:            
            return ['nan']

    

#----------------load hublist for looping---------------------------------------
hublist = get_data('rms','hublist.csv')

hublist['wakeup'] = hublist['HUB ID'].apply(lambda x: final_generator(x,'wakeup'))
#hublist['sleep'] = hublist['HUB ID'].apply(lambda x: final_generator(x,'sleep'))
print(hublist)

#hub_id = hublist['HUB ID'][0]
#room_acttime = get_data('temp3',hub_id)

