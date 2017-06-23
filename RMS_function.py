import pandas as pd
from datetime import datetime
from datetime import timedelta
from dateutil import parser
from datetime import date
import pytz
from pytz import timezone
from azure.storage.blob import BlobService
from io import StringIO
from decimal import Decimal

def setflag(timestamp,day,tz):
    flag = date.today()- timedelta(day)
    flag = flag.strftime('%Y-%m-%d')
    flag = flag + ' '+ timestamp
    flag = datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")
    if tz!='normal':
        flag = flag.replace(tzinfo=pytz.timezone('Asia/Singapore'))
    else:
        flag = flag
    return flag

def from_blob_load_data(account_name_,account_key_,container_name_,types):
    blob_service = BlobService(account_name=account_name_, account_key = account_key_)
    blobs = [];blob_date = []
    generator = blob_service.list_blobs(container_name_)
    for blob in generator:
        blobs.append(blob.name)
        blob_date.append(blob.name[:10])
    blob_table = pd.DataFrame()
    blob_table['date'] = blob_date
    blob_table['blobname'] = blobs    

    Today = date.today(); Today = Today.strftime('%Y-%m-%d')
    Yst = date.today() - timedelta(1) ; Yst = Yst.strftime('%Y-%m-%d')
    blob_table = blob_table[(blob_table['date']==Today)|(blob_table['date']==Yst)]    

    if blob_table.shape[0]>0:
        blob_df = pd.DataFrame()
        for blobname in blob_table['blobname']:
            blob_Class = blob_service.get_blob_to_text(container_name=container_name_, blob_name = blobname)
            blob_String =blob_Class
            blob_df1 = pd.read_csv(StringIO(blob_String),low_memory=False)
            blob_df = blob_df.append(blob_df1)        
        
        blob_df.index = range(blob_df.shape[0])
        print(blob_df.shape[0])
        if types=='PIR':
            blob_df = blob_df#[blob_df['tasklocation']=='Bathroom']
    else:
        blob_df = pd.DataFrame()
    return blob_df

def add_hubid2rmsdata(rms_raw):
    rms_raw.loc[(rms_raw['gwId']=='e47fb2f7ae63'),'hubid' ]= 'SG-04-avent001'
    rms_raw.loc[(rms_raw['gwId']=='e47fb2f7af1d'),'hubid' ]= 'SG-04-starhub6'
    rms_raw.loc[(rms_raw['gwId']=='e47fb2f7ae74'),'hubid' ]= 'SG-04-testingN'
    rms_raw.loc[(rms_raw['gwId']=='e47fb2f7adb0'),'hubid' ]= 'SG-04-testingQ'
    rms_raw.loc[(rms_raw['gwId']=='e47fb2f7ae5f'),'hubid' ]= 'SG-04-starhub8'
    rms_raw.loc[(rms_raw['gwId']=='e47fb2f7af61'),'hubid' ]= 'SG-04-inter002'
    rms_raw.loc[(rms_raw['gwId']=='e47fb2f7af25'),'hubid' ]= 'SG-04-inter001'
    rms_raw.loc[(rms_raw['gwId']=='e47fb2f7adae'),'hubid' ]= 'SG-04-testingK'
    rms_raw.loc[(rms_raw['gwId']=='e47fb2f7af68'),'hubid' ]= 'SG-04-hub00003'
    rms_raw.loc[(rms_raw['gwId']=='e47fb2f7af77'),'hubid' ]= 'SG-04-swaritAK'
    rms_raw.loc[(rms_raw['gwId']=='e47fb2f7afff'),'hubid' ]= 'SG-04-aylaayla'
    rms_raw.loc[(rms_raw['gwId']=='e47fb2f7af6f'),'hubid' ]= 'SG-04-starhub6'
    
    return rms_raw

def del_neighbor(data,types):
    if types == 'wakeup':
        data = data.drop_duplicates('time',keep='first')#.reset_index(drop=True)
        data = data.sort(['time'], ascending=[1])
        data.index = range(data.shape[0])
        data['gap'] = data['time'].diff()
        data['gap'].ix[0] = timedelta(seconds=1000)
        data['gap'] = data['gap'].apply(lambda x:x.seconds)
        data = data[data['gap']>300]
    if types == 'sleep':
        data = data.drop_duplicates('time',keep='last')#.reset_index(drop=True)
        data.index = range(data.shape[0])
        data = data.sort(['time'], ascending=[0])
        data['gap'] = data['time'].diff()
        data['gap'].ix[0] = timedelta(seconds=1000)
        data['gap'] = data['gap'].apply(lambda x:x.seconds)
        data = data[data['gap']>300]        
    return data

def remove_outliers(table):
    outliers = table.to_frame()
    outliers['time'] = outliers.index
    outliers = outliers[outliers[0]>11]
    outliers.index = range(outliers.shape[0])            
    i=1 ;n = table.shape[0]-1    
    while i<n:
        if table[i]>11:
            table[i]=max(table[i-1],table[i+1])
        i = i+1
    return table, outliers    

def time_picker(raw_wake_table,types,bathroom_table):
    if types=='sleep':
        flag3 = setflag("03:00:00",0,'normal'); flag4 = setflag("20:59:59",1,'normal')
        sleep = raw_wake_table[(raw_wake_table['time']<flag3)&(raw_wake_table['time']>flag4)]
        if sleep.shape[0]>1:
            sleep.index = range(sleep.shape[0])
            delta = sleep.at[1, 'time'] - sleep.at[0, 'time']
            if delta.seconds <2400:
                sleep = sleep.iloc[[1]]
            else :
                sleep = sleep.iloc[[0]]
            print(sleep)   
        if sleep.shape[0]==1:
            print(sleep)
        if sleep.shape[0]==0:
            print("hello, no data lah")
        sleep = sleep.rename(index=str, columns={ 'time': 'sleep'})
        return sleep
    if types =='wakeup':
        flag3 = setflag("05:00:00",0,'normal')
        wakeup = raw_wake_table[raw_wake_table['time']>flag3]
        if bathroom_table.shape[0]!=0:
            wakeup.index = range(wakeup.shape[0])
            flag1=wakeup.ix[0]['time'];flag2=wakeup.ix[1]['time']
            bathroom_table = bathroom_table[(bathroom_table['time']>flag1)&(bathroom_table['time']<flag2)]
        if wakeup.shape[0]>1:
            wakeup.index = range(wakeup.shape[0])
            delta = wakeup.at[1, 'time'] - wakeup.at[0, 'time']
            if (delta.seconds >2400)&(bathroom_table.shape[0]<3):
                wakeup = wakeup.iloc[[1]]
            else :
                wakeup = wakeup.iloc[[0]]
            print(wakeup)
        if wakeup.shape[0]==1:
            print(wakeup)
        if wakeup.shape[0]==0:
            print("hello, no data lah")
        wakeup = wakeup.rename(index=str, columns={ 'time': 'wakeup'})   
        return wakeup

def get_grouped(rawdata,types):
    rawdata = rawdata.set_index(rawdata['eventtime'].map(parser.parse))
    if types=='bathroom':
        room_list = rawdata['tasklocation'].unique().tolist()
        room_whole = pd.DataFrame()
        for room in room_list:
            room_frame = rawdata[rawdata['tasklocation']==room]
            room_frame = room_frame.groupby(pd.TimeGrouper('300s')).max()
            room_frame = room_frame[['value','tasklocation']]
            room_whole = room_whole.append(room_frame)
        room_whole = room_whole.dropna(subset=['value'], how='all')
        room_whole['time'] = room_whole.index
        return room_whole
    if types=='bedroom':
        if rawdata.shape[0]!=0:
            one_time_table = rawdata.groupby(pd.TimeGrouper('60s')).size() 
            five_time_table = rawdata.groupby(pd.TimeGrouper('300s')).size()    
            one_time_table,outliers = remove_outliers(one_time_table)
            time_table = one_time_table.to_frame()
            time_table = time_table.rename(index=str, columns={ 0: "1min"})
            time_table['sum'] = pd.rolling_sum(time_table['1min'],5)
            time_table.loc[(time_table['sum']<12),'sum']= 0
            time_table.loc[(time_table['sum']>12),'sum']= time_table['sum']
            #time_table = time_table.rename(index=str, columns={ 2: "5min"})
            time_table = time_table.set_index(time_table.index.map(parser.parse))
            awake_table = time_table.groupby(pd.TimeGrouper('300s')).max()
            awake_table['5min'] = five_time_table
            awake_table['time'] = awake_table.index
            awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
            awake_table['gap'] = awake_table['5min'].diff()
            awake_table = awake_table[(awake_table['sum']>12)|(awake_table['gap']<-7)]
            awake_table['time_gap'] = awake_table['time'].diff()
        else:
            awake_table = pd.DataFrame()
        return awake_table

def time_normer(rawdata):
    rawdata = rawdata.dropna(subset=['starttime'],how='all')
    rawdata['time'] = rawdata['starttime'].apply(lambda x: str(x))
    rawdata['time'] = rawdata['time'].apply(lambda x: x[:19])
    rawdata['time'] = rawdata['time'].apply(lambda x: datetime.strptime(x,'%Y-%m-%dT%H:%M:%S')+timedelta(hours=8))
    rawdata['eventtime'] = rawdata['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')) 
    flag3 = setflag("09:00:01",0,'normal'); flag4 = setflag("19:59:59",1,'normal')
    rawdata = rawdata[(rawdata['time']<flag3)&(rawdata['time']>flag4)]
    return rawdata

def final_generator(hub_id,types,rms_whole_input,blob_df_whole_input):
    rms_data = rms_whole_input[rms_whole_input['hubid']==hub_id]
    blob_df = blob_df_whole_input[blob_df_whole_input['deviceid']==hub_id]
    awake_table = get_grouped(rms_data,'bedroom','60s')
    #------------------------------------------------------if there is bathroom data--------------------------------------------
    if blob_df.shape[0]!=0:     
        bathroom_time = get_grouped(blob_df,'bathroom','600s')    
        #----------add RMS data--------------------------------------------------
        if awake_table.shape[0]>1:
            awake_table['time'] = awake_table.index
            awake_table.index = range(awake_table.shape[0])
            flag = awake_table['time'];flag.index = range(flag.shape[0])
            flag1 = flag[0]#;k = len(flag)-1;flag2 = flag[k] 
            bathroom_time = bathroom_time[bathroom_time['time']>flag1]
            awake_table = awake_table.append(bathroom_time)
            awake_table = del_neighbor(awake_table)
            #-------use HV table filter the bathroom data 
            #-------------deal with HV talbe & select the wake up time--------------------------
            awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
            wakeup = time_picker(awake_table,'wakeup',bathroom_time)
            #-------------deal with HV talbe & select the sleep time----------------------------
            sleep = time_picker(awake_table,'sleep',bathroom_time)
        if awake_table.shape[0]==1:
            awake_table['time'] = awake_table.index
            awake_table.index = range(awake_table.shape[0])
            flag = awake_table['time'];flag.index = range(flag.shape[0])
            flag1 = flag[0];flag2=setflag("05:00:00",0,'normal')
            if flag1<flag2:
                  bathroom_time = bathroom_time[bathroom_time['time']>flag1]
            if flag1>flag2:
                  flag3 = setflag("20:59:59",1,'normal')
                  bathroom_time = bathroom_time[bathroom_time['time']>flag3]
            awake_table = awake_table.append(bathroom_time)
            awake_table = del_neighbor(awake_table)      
            #-------------deal with HV talbe & select the wake up time--------------------------
            awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
            wakeup = time_picker(awake_table,'wakeup',bathroom_time)
            #-------------deal with HV talbe & select the sleep time----------------------------
            sleep = time_picker(awake_table,'sleep',bathroom_time)
        if awake_table.shape[0]==0:
            sleep = pd.DataFrame(); sleep['sleep'] = 'nan'
            wakeup = pd.DataFrame(); wakeup['wakeup'] = 'nan'            
    #---------------------------------------------------------if do not have bathroom data--------------------------------------------
    if blob_df.shape[0]==0:
        bathroom_time = pd.DataFrame()
        if awake_table.shape[0]>1:
            awake_table['time'] = awake_table.index
            awake_table = del_neighbor(awake_table)
            #-------------deal with HV talbe & select the wake up time-------------------------------------
            print(len(awake_table))
            awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
            #--------------------deal with sleep time based on HV table------------------------------------
            wakeup = time_picker(awake_table,'wakeup',bathroom_time)
            #-------------deal with HV talbe & select the sleep time---------------------------------------
            sleep = time_picker(awake_table,'sleep',bathroom_time)
        else:
            sleep = pd.DataFrame(); sleep['sleep'] = 'nan'
            wakeup = pd.DataFrame(); wakeup['wakeup'] = 'nan'
    if (sleep.shape[0]!=0)&(types=='sleep'):
        return sleep.ix[0]['sleep']
    if (wakeup.shape[0]!=0)&(types=='wakeup'):
        return wakeup.ix[0]['wakeup']
    else:
        return ['nan']


def get_grouped_bathroom(rawdata,epoch):
    rawdata = rawdata.set_index(rawdata['eventtime'].map(parser.parse))
    bathroom_time = rawdata.groupby(pd.TimeGrouper(epoch)).max()
    bathroom_time = bathroom_time.dropna(subset=['value'], how='all')
    bathroom_time['eventtime'] = bathroom_time.index
    bathroom_time.index = range(bathroom_time.shape[0])
    bathroom_time = bathroom_time[['time']]
    bathroom_time['1min'] = 8;bathroom_time['sum'] = 0;bathroom_time['5min'] = 0
    return bathroom_time
'''
def up2blob(account_name_input,account_key_input,container_name_input = 'rmsoutput',uploadfile):
    
    #uploadfile should be a dataframe
    
    append_blob_service = AppendBlobService(account_name=account_name_input, account_key=account_key_input)
    append_blob_service.create_container(container_name_input)
    sleep_text = uploadfile.to_csv()
    Today = date.today(); Today = Today.strftime('%Y-%m-%d')
    append_blob_service.create_blob(container_name, Today)
    append_blob_service.append_blob_from_text(container_name, Today, sleep_text)

'''
