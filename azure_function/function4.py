import sys, os.path
import os
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'myvenv/Lib/site-packages')))

import pandas as pd
from datetime import datetime
from datetime import timedelta
from datetime import date
from azure.storage.blob import BlockBlobService
from io import StringIO
from azure.storage.blob import AppendBlobService

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
    #---------------------sleep part---------------------------
    if types=='sleep':
        flag3 = setflag("01:30:01",day_now); flag4 = setflag("20:59:59",day_before)
        sleep = raw_wake_table[(raw_wake_table['time']<flag3)&(raw_wake_table['time']>flag4)]
        if sleep.shape[0]==0:
            #sleep = sleep 
            print("no sleep data")
        #if sleep.shape[0]==1:
        #    sleep = sleep
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

def time_picker_single(raw_wake_table, types,rms_data_input):
    if types=='sleep':
        flag3 = setflag("02:00:01",day_now); flag4 = setflag("22:59:59",day_before)
        test = raw_wake_table[(raw_wake_table['time']<flag3)&(raw_wake_table['time']>flag4)]
        sleep = pd.DataFrame()
        if test.shape[0]>0:
            flag3 = setflag("02:00:01",day_now); flag4 = setflag("22:59:59",day_before)
            sleep = raw_wake_table[(raw_wake_table['time']<flag3)&(raw_wake_table['time']>flag4)]
            sleep.index = range(sleep.shape[0])
            if sleep.shape[0]!=0:
                sleep = sleep.loc[[0]]
            if sleep.shape[0]==0:
                flag3 = setflag("23:00:00",day_before); flag4 = setflag("19:59:59",day_before)
                sleep = raw_wake_table[(raw_wake_table['time']<flag3)&(raw_wake_table['time']>flag4)]
                flag4 = setflag("05:00:00",day_now)
                sleep = sleep[sleep['time']<flag4]
                if sleep.shape[0]!=0:
                    sleep = sleep.sort('time',ascending = 0)
                    sleep.index = range(sleep.shape[0])
                    sleep = sleep.loc[[0]]
                if sleep.shape[0]==0:
                    flag3 = setflag("03:00:00",day_now); flag4 = setflag("02:00:00",day_now)
                    sleep = raw_wake_table[(raw_wake_table['time']<flag3)&(raw_wake_table['time']>flag4)]
                    if sleep.shape[0]==0:
                        print("no sleeping data")
                        sleep = pd.DataFrame()
                    if sleep.shape[0]!=0:
                        sleep.index = range(sleep.shape[0])
                        sleep = sleep.loc[[0]]
            sleep = sleep.rename(index=str, columns={ 'time': 'sleep'})
        if test.shape[0]==0:
            flag3 = setflag("02:00:01",day_now)
            raw_wake_table = raw_wake_table[raw_wake_table['time']<flag3]
            raw_wake_table.index = range(raw_wake_table.shape[0])
            sleep = raw_wake_table[-1:]
            sleep.index = range(sleep.shape[0])
            sleep = sleep.rename(index=str, columns={ 'time': 'sleep'})
        return sleep



def get_data(containername,blobname):
    
    blob_Class_ = blob_service_.get_blob_to_text(container_name=containername, blob_name = blobname)
    blob_String_ =blob_Class_.content
    result = pd.read_csv(StringIO(blob_String_),low_memory=False) 
    return result

def final_generator(hub_id,types):
    rms_data = rms_data_whole[rms_data_whole['hubid']==hub_id]
    
    if types == 'wakeup':
        room_acttime = pd.DataFrame()
        awake_table = awake_table_wakeup[awake_table_wakeup['deviceid']==hub_id]
        if len(awake_table)>0:
            awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
            wakeup = time_picker(awake_table,types,rms_data,room_acttime)
        else:
            pass
        try:            
            return wakeup.ix[0]['wakeup']
        except:            
            return ['nan']
    #-----------------------------sleep part-------------------------------------
    if types == 'sleep':
        
        awake_table = awake_table_sleep[awake_table_sleep['deviceid']==hub_id]
        
        room_acttime = room_acttime_whole[room_acttime_whole['deviceid']==hub_id]
        if len(room_acttime)>0:
            awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
            sleep = time_picker(awake_table,types,rms_data,room_acttime)
        else:
            awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
            sleep = time_picker_single(awake_table, 'sleep', rms_data)
        try:            
            return sleep.ix[0]['sleep']
        except:            
            return ['nan']
    

#----------------load hublist for looping---------------------------------------
hublist = get_data('rms','hublist.csv')

rms_data_whole = get_data('function', "rms_raw_clean")
awake_table_wakeup = get_data('function',"awake_table_wakeup")
awake_table_sleep = get_data('function',"awake_table_sleep")
room_acttime_whole = get_data('function',"pir_grouped")

rms_data_whole['time'] = rms_data_whole['time'].apply(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))
awake_table_wakeup['time'] = awake_table_wakeup['time'].apply(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))
awake_table_sleep['time'] = awake_table_sleep['time'].apply(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))
room_acttime_whole['time'] = room_acttime_whole['time'].apply(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))  


hublist['wakeup'] = hublist['HUB ID'].apply(lambda x: final_generator(x,'wakeup'))
hublist['sleep'] = hublist['HUB ID'].apply(lambda x: final_generator(x,'sleep'))
print(hublist['sleep'])

append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)

Today = date.today().strftime('%Y-%m-%d')
append_blob_service.create_blob('rmsoutput', Today)
append_blob_service.append_blob_from_text('rmsoutput', Today, hublist.to_csv())

'''
hub = hublist['HUB ID'][1]
final_generator(hub,'sleep')

awake_table = awake_table_sleep[awake_table_sleep['deviceid']==hub]
room_acttime_whole = get_data('function',"pir_grouped")
room_acttime = room_acttime_whole[room_acttime_whole['deviceid']==hub]
'''

