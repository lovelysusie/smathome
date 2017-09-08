import pandas as pd
from datetime import datetime
from datetime import timedelta
from dateutil import parser
from datetime import date
import pytz
from azure.storage.blob import BlockBlobService
from io import StringIO
day_now=0
day_before=1

def setflag(timestamp,day,tz):
    flag = date.today()- timedelta(day)
    flag = flag.strftime('%Y-%m-%d')
    flag = flag + ' '+ timestamp
    flag = datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")
    return flag


def del_neighbor(data,types):
    data = data.drop_duplicates('time',keep='first')#.reset_index(drop=True)
    data = data.sort_values(['time'], ascending=[1])
    data.index = range(data.shape[0])       
    if types == 'wakeup':
        data['gap'] = data['time'].diff()
        data['gap'].ix[0] = timedelta(seconds=1000)
        data['gap'] = data['gap'].apply(lambda x:x.seconds)
        data = data[data['gap']>300]
    if types == 'sleep':
        test = data['time'].diff().to_frame()  
        test = test.drop(test.index[[0]])
        test['time'] = test['time'].apply(lambda x: x.seconds)
        test = test['time'].tolist() 
        test.append(300)        
        data['gap'] = test
        data = data[data['gap']>400]
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

def get_agv_int(rms_data_input):
    rms_data_input['eventtime'] = rms_data_input['eventtime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))  
    intensity = rms_data_input.set_index('eventtime').groupby(pd.TimeGrouper(freq='5Min')).size()  
    intensity = intensity.to_frame()
    intensity = intensity[intensity[0]!=0]  
    output = intensity[0].mean()    
    return output

def time_picker(raw_wake_table,types,rms_data_input,room_acttime_input):
    if types=='sleep':
        flag3 = setflag("01:30:01",day_now,'normal'); flag4 = setflag("20:59:59",day_before,'normal')
        sleep = raw_wake_table[(raw_wake_table['time']<flag3)&(raw_wake_table['time']>flag4)]
        if sleep.shape[0]==0:
            sleep = sleep; print("no sleep data")
        if sleep.shape[0]==1:
            sleep = sleep
        if sleep.shape[0]==2:
            if (sleep.at[sleep.index[1], 'time']-sleep.at[sleep.index[0], 'time']).seconds>=2400:
                sleep.at[sleep.index[0], 'time'] = sleep.at[sleep.index[0], 'time']+timedelta(minutes=10)
            check_frame = room_acttime_input
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
        flag3 = setflag("05:00:00",day_now,'normal')
        wakeup = raw_wake_table[raw_wake_table['time']>flag3]
        if wakeup.shape[0]>3:
            wakeup = wakeup[wakeup['gap']!=600]
        if wakeup.shape[0]==0:
            wakeup = wakeup; print("no wake up data")
        if wakeup.shape[0]==1:
            wakeup = wakeup
        if wakeup.shape[0]==2:
            wakeup.index = range(wakeup.shape[0])
            rms_data_check = rms_data_input[(rms_data_input['time']>(wakeup.at[0,'time']+timedelta(minutes=5)))&(rms_data_input['time']<(wakeup.at[1,'time']-timedelta(minutes=5)))]
            if rms_data_check.shape[0]>4:
                agv_int = get_agv_int(rms_data_check)
                if agv_int<=5:
                    wakeup = wakeup.loc[[1]]
                if agv_int>5:
                    wakeup = wakeup.loc[[0]]
            if rms_data_check.shape[0] <= 4:
                wakeup = wakeup.loc[[0]]
        if wakeup.shape[0]>2:
            wakeup.index = range(wakeup.shape[0])
            rms_data_check = rms_data_input[(rms_data_input['time']>(wakeup.at[0,'time']+timedelta(minutes=5)))&(rms_data_input['time']<(wakeup.at[1,'time']-timedelta(minutes=5)))]                
            if rms_data_check.shape[0]>4:
                agv_int = get_agv_int(rms_data_check)
                if agv_int<=5:
                    rms_data_check2 = rms_data_input[(rms_data_input['time']>(wakeup.at[1,'time']+timedelta(minutes=5)))&(rms_data_input['time']<(wakeup.at[2,'time']-timedelta(minutes=5)))]
                    if rms_data_check2.shape[0]>5:
                        agv_int2 = get_agv_int(rms_data_check2)
                        if agv_int2<=5:
                            wakeup = wakeup.loc[[2]]
                        if agv_int2 >5:
                            wakeup = wakeup.loc[[1]]
                    if rms_data_check2.shape[0]<=5:
                        wakeup = wakeup.loc[[1]]
                if agv_int>5:
                    wakeup = wakeup.loc[[0]]
            if rms_data_check.shape[0] <= 4:
                wakeup = wakeup.loc[[0]]
        wakeup = wakeup.rename(index=str, columns={ 'time': 'wakeup'})   
        return wakeup

def get_check(raw_rms_dara,wakeup_input):
    '''
    This function is going to generate the checking frame of other rooms
    '''
    raw_rms_dara = raw_rms_dara.set_index(raw_rms_dara['eventtime'].map(parser.parse))
    five_time_table = raw_rms_dara.groupby(pd.TimeGrouper('300s')).size()
    five_time_table = five_time_table.to_frame()
    five_time_table = five_time_table.rename(index=str, columns={ 0: "sum"})
    five_time_table['time'] = five_time_table.index
    five_time_table['time'] = five_time_table['time'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
    five_time_table = five_time_table[(five_time_table['time']>wakeup_input.at[0, 'time'])&(five_time_table['time']<wakeup_input.at[1, 'time'])]
    five_time_table = five_time_table[five_time_table['sum']==0]
    five_time_table['gap'] = 0
    five_time_table = five_time_table.append(wakeup_input[:2])
    five_time_table = five_time_table.sort_values('time',ascending = 1)
    five_time_table['gap'] = five_time_table['time'].diff()
    five_time_table['gap'].ix[0] = timedelta(seconds=300)
    five_time_table['gap'] = five_time_table['gap'].apply(lambda x: x.seconds)  
    return five_time_table
    

def time_picker_single(raw_wake_table, types,rms_data_input):
    if types=='sleep':
        flag3 = setflag("02:00:01",day_now,'normal'); flag4 = setflag("22:59:59",day_before,'normal')
        test = raw_wake_table[(raw_wake_table['time']<flag3)&(raw_wake_table['time']>flag4)]
        sleep = pd.DataFrame()
        if test.shape[0]>0:
            flag3 = setflag("02:00:01",day_now,'normal'); flag4 = setflag("22:59:59",day_before,'normal')
            sleep = raw_wake_table[(raw_wake_table['time']<flag3)&(raw_wake_table['time']>flag4)]
            sleep.index = range(sleep.shape[0])
            if sleep.shape[0]!=0:
                sleep = sleep.loc[[0]]
            if sleep.shape[0]==0:
                flag3 = setflag("23:00:00",day_before,'normal'); flag4 = setflag("19:59:59",day_before,'normal')
                sleep = raw_wake_table[(raw_wake_table['time']<flag3)&(raw_wake_table['time']>flag4)]
                if sleep.shape[0]!=0:
                    sleep = sleep.sort('time',ascending = 0)
                    sleep.index = range(sleep.shape[0])
                    sleep = sleep.loc[[0]]
                if sleep.shape[0]==0:
                    flag3 = setflag("03:00:00",day_now,'normal'); flag4 = setflag("02:00:00",day_now,'normal')
                    sleep = raw_wake_table[(raw_wake_table['time']<flag3)&(raw_wake_table['time']>flag4)]
                    if sleep.shape[0]==0:
                        print("no sleeping data")
                        sleep = pd.DataFrame()
                    if sleep.shape[0]!=0:
                        sleep.index = range(sleep.shape[0])
                        sleep = sleep.loc[[0]]
            sleep = sleep.rename(index=str, columns={ 'time': 'sleep'})
        if test.shape[0]==0:
            raw_wake_table.index = range(raw_wake_table.shape[0])
            sleep = raw_wake_table[-1:]
            sleep.index = range(sleep.shape[0])
            sleep = sleep.rename(index=str, columns={ 'time': 'sleep'})
        return sleep

    if types =='wakeup':
        flag3 = setflag("05:00:00",day_now,'normal')
        wakeup = raw_wake_table[raw_wake_table['time']>flag3]
        if wakeup.shape[0]>1:
            wakeup.index = range(wakeup.shape[0])
            check_frame = get_check(rms_data_input,wakeup)
            check_frame = check_frame[check_frame['gap']==300]
            if wakeup.shape[0]==2:
                if check_frame.shape[0]>2:
                    wakeup = wakeup.iloc[[0]]
                if check_frame.shape[0]<3:
                    wakeup = wakeup.iloc[[1]]
            if wakeup.shape[0]>2:
                check_frame = get_check(rms_data_input,wakeup)
                check_frame = check_frame[check_frame['gap']==300]
                if check_frame.shape[0]>2:
                    wakeup = wakeup.iloc[[0]]
                if check_frame.shape[0]<3:
                    wakeup.drop(wakeup.index[0], inplace=True)
                    wakeup.index = range(wakeup.shape[0])
                    check_frame = get_check(rms_data_input,wakeup)
                    check_frame = check_frame[check_frame['gap']==300]
                    if check_frame.shape[0]>2:
                        wakeup = wakeup.iloc[[0]]
                    if check_frame.shape[0]<3:
                        wakeup = wakeup.iloc[[1]]
        if wakeup.shape[0]==1:
            print(wakeup)
        if wakeup.shape[0]==0:
            print("hello, no data lah")
        wakeup = wakeup.rename(index=str, columns={ 'time': 'wakeup'})   
        return wakeup

def bathroon_checker(bathroom_input):
    # This function is going to eleminate the sunddently appear value 1 bathroom time stamp
    bathroom_input['gap'] = bathroom_input['time'].diff() 
    bathroom_input['gap'][0] = timedelta(minutes = 5)
    bathroom_input['gap'] = bathroom_input['gap'].apply(lambda x: x.seconds)
    bathroom_input = bathroom_input[(bathroom_input['sum']!=1)|(bathroom_input['gap']==300)]
    return bathroom_input


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
    if bathroom_time.shape[0]>0:
        bathroom_time = bathroon_checker(bathroom_time)
    if bathroom_time.shape[0]==0:
        bathroom_time = bathroom_time
    return bathroom_time


def get_data(containername,blobname):
    blob_service_ = BlockBlobService(account_name=account_name, account_key = account_key)
    blob_Class_ = blob_service_.get_blob_to_text(container_name=containername, blob_name = blobname)
    blob_String_ =blob_Class_.content
    room_acttime = pd.read_csv(StringIO(blob_String_),low_memory=False)
    return room_acttime

def final_generator(hub_id,types):
    awake_table = get_data('temp1',hub_id)
    room_acttime = get_data('temp2',hub_id)
    if awake_table.shape[0]!=0:
        awake_table['time'] = awake_table['time'].apply(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))
    if room_acttime.shape[0]!=0:
        room_acttime['time'] = room_acttime['time'].apply(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))  
    rms_data = get_data('temp3',hub_id)
    if rms_data.shape[0]!=0:
        rms_data['time'] = rms_data['time'].apply(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))  
    if room_acttime.shape[0]!=0:
        print("multiroom user")    
        if types=='wakeup':
            if awake_table.shape[0]>1:
                flag = awake_table['time'];flag.index = range(flag.shape[0])
                flag1 = flag[0]#;k = len(flag)-1;flag2 = flag[k]
                bathroom_time = bathroom_table_prep(room_acttime)
                bathroom_time = bathroom_time[bathroom_time['time']>flag1]
                awake_table = awake_table.append(bathroom_time)
                awake_table = del_neighbor(awake_table,'wakeup')
                awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
                wakeup = time_picker(awake_table,'wakeup',rms_data,room_acttime)
            
            if awake_table.shape[0]==1:
                flag = awake_table['time'];flag.index = range(flag.shape[0])
                bathroom_time = bathroom_table_prep(room_acttime)
                flag1 = flag[0];flag2=setflag("05:00:00",day_now,'normal')
                if flag1<flag2:
                      bathroom_time = bathroom_time[bathroom_time['time']>flag1]
                if flag1>flag2:
                      flag3 = setflag("20:59:59",day_before,'normal')
                      bathroom_time = bathroom_time[bathroom_time['time']>flag3]
                awake_table = awake_table.append(bathroom_time)
                awake_table = del_neighbor(awake_table,'wakeup')      
                awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
                wakeup = time_picker(awake_table,'wakeup',rms_data,room_acttime)
            if awake_table.shape[0]==0:
                bathroom_time = bathroom_table_prep(room_acttime)
                if bathroom_time.shape[0]!=0:
                    bathroom_time = del_neighbor(bathroom_time,'wakeup')
                    wakeup = time_picker(bathroom_time,'wakeup',rms_data,room_acttime)
                if bathroom_time.shape[0]==0:    
                    wakeup = pd.DataFrame(); wakeup['wakeup'] = 'nan'            
        if types=='sleep':
            bathroom_time = bathroom_table_prep(room_acttime)
            if awake_table.shape[0]!=0:
                awake_table = awake_table[awake_table['sum']>10]
            if awake_table.shape[0]>0:
                awake_table = awake_table.append(bathroom_time)
                awake_table = del_neighbor(awake_table,'sleep')
                awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
                sleep = time_picker(awake_table,'sleep',bathroom_time,rms_data,room_acttime)
            if awake_table.shape[0]==0:
                if bathroom_time.shape[0]!=0:
                    bathroom_time = del_neighbor(bathroom_time,'sleep') 
                    sleep = time_picker(bathroom_time,'sleep',rms_data,room_acttime)
                if bathroom_time.shape[0]==0:
                    sleep = pd.DataFrame(); sleep['wakeup'] = 'nan'
    if room_acttime.shape[0]==0:
        print("RMS only")
        bathroom_time = pd.DataFrame()
        if awake_table.shape[0]>1:
            #awake_table['time'] = awake_table.index
            awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
            if types=='wakeup':
                awake_table = del_neighbor(awake_table,'wakeup')
                wakeup = time_picker_single(awake_table,'wakeup',rms_data)
            if types=='sleep':
                #awake_table = awake_table[awake_table['sum']>12]
                awake_table = del_neighbor(awake_table,'sleep')
                sleep = time_picker_single(awake_table,'sleep',rms_data)
        else:
            sleep = pd.DataFrame(); sleep['sleep'] = 'nan'
            wakeup = pd.DataFrame(); wakeup['wakeup'] = 'nan'
    if types=='sleep':
        if sleep.shape[0]!=0:
            return sleep.ix[0]['sleep']
        if sleep.shape[0]==0:
            return ['nan']        
    if types=='wakeup':
        if wakeup.shape[0]!=0:            
            return wakeup.ix[0]['wakeup']
        if wakeup.shape[0]==0:            
            return ['nan']


account_name='####'
account_key='####'
#----------------load hublist for looping---------------------------------------
hublist = get_data('rms','hublist.csv')

hublist['wakeup'] = hublist['HUB ID'].apply(lambda x: final_generator(x,'wakeup'))
hublist['sleep'] = hublist['HUB ID'].apply(lambda x: final_generator(x,'sleep'))
print(hublist)
