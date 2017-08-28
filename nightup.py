# -*- coding: utf-8 -*-
"""
Created on Thu Aug  3 10:41:28 2017

@author: Susie
"""
from azure.storage.blob import BlobService
import pandas as pd
from io import StringIO
from datetime import date, timedelta
from datetime import datetime
from decimal import Decimal
import RMS_function as RMS
#from azure.storage.blob import AppendBlobService

def get_wake_tms(hub_id,i):
    rms_data = rms_whole[rms_whole['hubid']==hub_id]
    blob_df = blob_df_whole[blob_df_whole['deviceid']==hub_id]
    print(hub_id)
    if blob_df.shape[0]!=0:     
        room_acttime = RMS.get_grouped(blob_df,'bathroom')
        print("multiroom user")
        awake_table = RMS.get_grouped(rms_data,'bedroom')
        if len(awake_table)>0:
            flag = awake_table['time'];flag.index = range(flag.shape[0])
            flag1 = flag[0]#;k = len(flag)-1;flag2 = flag[k]
            bathroom_time = RMS.bathroom_table_prep(room_acttime)    
            bathroom_time = bathroom_time[bathroom_time['time']>flag1]
            awake_table = awake_table[awake_table['sum']>11]
            awake_table = awake_table.append(bathroom_time)
            if awake_table.shape[0]>1:
                awake_table = RMS.del_neighbor(awake_table,'wakeup')
        if len(awake_table)==0:
            bathroom_time = RMS.bathroom_table_prep(room_acttime)
            awake_table = bathroom_time
    if blob_df.shape[0]==0:
        print("RMS only")
        awake_table = RMS.get_grouped(rms_data,'bedroom')
        if len(awake_table)>2:
            awake_table = awake_table[awake_table['sum']>11]
        if len(awake_table)>2:
            awake_table = RMS.del_neighbor(awake_table,'wakeup')
    if awake_table.shape[0]>0:
        flag1 = hublist.ix[i]['sleep'] ; flag2 = hublist.ix[i]['wakeup']    
        awake_table = awake_table[(awake_table['time']>flag1)&(awake_table['time']<flag2)]
    tm = len(awake_table)
    return tm    

#from azure.storage.blob import PublicAccess
account_name='blobsensordata'
account_key='zUYv9mIC9KPr/k+Sa15y4mN6mtozuJcF/n979cqojT4HaMUj3ahEHaPBVtpDihwfO78JTk8sQ29xCaxGWfjtSA=='
#----------------load hublist for looping---------------------------------------
container_name = 'rmsoutput'
blob_service_ = BlobService(account_name=account_name, account_key = account_key)
    
k = 0; n = 5
while k<n:
    RMS.day_now = k
    RMS.day_before = k+1
    today = date.today() - timedelta(days=k)  
    today = today.strftime('%Y-%m-%d')    
    blobs_ = today
    container_name = 'rmsoutput'
    blob_Class_ = blob_service_.get_blob_to_text(container_name=container_name, blob_name = blobs_)
    blob_String_ =blob_Class_
    hublist = pd.read_csv(StringIO(blob_String_),low_memory=False)    
    hublist = hublist[(hublist['sleep']!="['nan']")&(hublist['wakeup']!="['nan']")]
    hublist['sleep'] = hublist['sleep'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S')) 
    hublist['wakeup'] = hublist['wakeup'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))     
    #------------------------------------load the whole rms data with many device-----------------------------------------
    container_name = 'rmsinput'
    rms_whole = RMS.from_blob_load_data(account_name_ = account_name,account_key_=account_key,container_name_=container_name,types='rms')
    rms_whole = rms_whole[['gwId','EventProcessedUtcTime']]
    rms_whole = rms_whole.rename(index=str, columns={ 'EventProcessedUtcTime': "starttime"})
    rms_whole = RMS.time_normer(rms_whole)
    rms_whole = RMS.add_hubid2rmsdata(rms_whole)    
    #----------------------------------from blob import Bathroom Data---------------------------------------------------------
    container_name = 'adventisdatainput'
    blob_df_whole = RMS.from_blob_load_data(account_name,account_key,container_name,'PIR')
    blob_df_whole = RMS.time_normer(blob_df_whole)
    #-----------------------------select the data within sleeping period---------------------
    #hub_id = 'SG-04-testingN'#wakeup
    hublist.index = range(hublist.shape[0])    
    #hub_id = 'SG-04-hub00019'
    #hublist['wpatnight'] = hublist.apply(lambda x: get_wake_tms(x['HUB ID'],x['NO']))    
    result = []
    i = 0 ; n = len(hublist)
    while i <n:
        result.append(get_wake_tms(hublist.ix[i]['HUB ID'],i))
        i = i+1    
    hublist['wakeup@night'] = result
    hublist.to_csv('/Users/Susie/Desktop/result/'+today+'.csv')
    #append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)
    #container_name = 'wakeupatnight'
    #append_blob_service.create_container(container_name)
    #sleep_text = hublist.to_csv()
    #append_blob_service.create_blob(container_name, today)
    #append_blob_service.append_blob_from_text(container_name, today, sleep_text)
    k = k+1
'''
#-------rehersal area-----------
hub_id = 'SG-04-testingN'
    rms_data = rms_whole[rms_whole['hubid']==hub_id]
    blob_df = blob_df_whole[blob_df_whole['deviceid']==hub_id]
    print(hub_id)
    if blob_df.shape[0]!=0:     
        room_acttime = RMS.get_grouped(blob_df,'bathroom')
        print("multiroom user")
        awake_table = RMS.get_grouped(rms_data,'bedroom')
        if len(awake_table)>0:
            flag = awake_table['time'];flag.index = range(flag.shape[0])
            flag1 = flag[0]#;k = len(flag)-1;flag2 = flag[k]
            bathroom_time = RMS.bathroom_table_prep(room_acttime)    
            bathroom_time = bathroom_time[bathroom_time['time']>flag1]
            awake_table = awake_table[awake_table['sum']>11]
            awake_table = awake_table.append(bathroom_time)
            if awake_table.shape[0]>1:
                awake_table = RMS.del_neighbor(awake_table,'wakeup')
        if len(awake_table)==0:
            bathroom_time = RMS.bathroom_table_prep(room_acttime)
            awake_table = bathroom_time
    if blob_df.shape[0]==0:
        print("RMS only")
        awake_table = RMS.get_grouped(rms_data,'bedroom')
        if len(awake_table)>2:
            awake_table = awake_table[awake_table['sum']>11]
        if len(awake_table)>2:
            awake_table = RMS.del_neighbor(awake_table,'wakeup')
    if awake_table.shape[0]>0:
        flag1 = hublist.ix[i]['sleep'] ; flag2 = hublist.ix[i]['wakeup']    
        awake_table = awake_table[(awake_table['time']>flag1)&(awake_table['time']<flag2)]
    tm = len(awake_table)


#flag3 = setflag("09:00:01",day_now,'normal'); flag4 = setflag("19:59:59",day_before,'normal')

#flag3>flag4

def time_normer(rawdata):
    rawdata = rawdata.dropna(subset=['starttime'],how='all')
    rawdata['time'] = rawdata['starttime'].apply(lambda x: str(x))
    rawdata['time'] = rawdata['time'].apply(lambda x: x[:19])
    rawdata['time'] = rawdata['time'].apply(lambda x: datetime.strptime(x,'%Y-%m-%dT%H:%M:%S')+timedelta(hours=8))
    rawdata['eventtime'] = rawdata['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')) 
    flag3 = setflag("09:00:01",day_now,'normal'); flag4 = setflag("19:59:59",day_before,'normal')
    rawdata = rawdata[(rawdata['time']<flag3)&(rawdata['time']>flag4)]
    return rawdata


blob_service = BlobService(account_name=account_name, account_key = account_key)
blobs = [];blob_date = []
generator = blob_service.list_blobs(container_name)
    for blob in generator:
        blobs.append(blob.name)
        blob_date.append(blob.name[:10])
    blob_table = pd.DataFrame()
    blob_table['date'] = blob_date
    blob_table['blobname'] = blobs    

    Today = date.today()- timedelta(days=day_now); Today = Today.strftime('%Y-%m-%d')
    Yst = date.today() - timedelta(day_now) ; Yst = Yst.strftime('%Y-%m-%d')
    blob_table = blob_table[(blob_table['date']==Today)|(blob_table['date']==Yst)]    


'''