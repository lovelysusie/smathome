import pandas as pd
from datetime import datetime
from datetime import timedelta
from dateutil import parser
from datetime import date
from azure.storage.blob import BlockBlobService
from io import StringIO
day_now=0
day_before=1

def setflag(timestamp,day):
    flag = date.today()- timedelta(day)
    flag = flag.strftime('%Y-%m-%d')
    flag = flag + ' '+ timestamp
    return datetime.strptime(flag, "%Y-%m-%d %H:%M:%S")

def from_blob_load_data(account_name_,account_key_,container_name_,types):
    blob_service = BlockBlobService(account_name=account_name_, account_key = account_key_)
    blobs = [];blob_date = []
    generator = blob_service.list_blobs(container_name_)
    for blob in generator:
        blobs.append(blob.name)
        blob_date.append(blob.name[:10])
    blob_table = pd.DataFrame()
    blob_table['date'] = blob_date
    blob_table['blobname'] = blobs    

    Today = date.today().strftime('%Y-%m-%d')
    Yst = (date.today() - timedelta(1)).strftime('%Y-%m-%d')
    blob_table = blob_table[(blob_table['date']==Today)|(blob_table['date']==Yst)]    

    if blob_table.shape[0]>0:
        blob_df = pd.DataFrame()
        for blobname in blob_table['blobname']:
            blob_Class = blob_service.get_blob_to_text(container_name=container_name_, blob_name = blobname)
            blob_String =blob_Class.content
            blob_df1 = pd.read_csv(StringIO(blob_String),low_memory=False)
            blob_df = blob_df.append(blob_df1)        
        
        blob_df.index = range(blob_df.shape[0])
        #print(blob_df.shape[0])
        if types=='PIR':
            blob_df = blob_df#[blob_df['tasklocation']=='Bathroom']
    else:
        blob_df = pd.DataFrame()
    return blob_df

def add_hubid2rmsdata(rms_raw):
    
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7ae74' ),'hubid' ] = 'SG-04-testingN'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af6a' ),'hubid' ] = 'SG-04-hub00016'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af1d' ),'hubid' ] = 'SG-04-develop4'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af1b' ),'hubid' ] = 'SG-04-testingQ'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af25' ),'hubid' ] = 'SG-04-inter001'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7adae' ),'hubid' ] = 'SG-04-testingK'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af42' ),'hubid' ] = 'SG-04-hub00001'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af7d' ),'hubid' ] = 'SG-04-hub00004'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af70' ),'hubid' ] = 'SG-04-hub00005'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af68' ),'hubid' ] = 'SG-04-hub00013'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af4a' ),'hubid' ] = 'SG-04-hub00006'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af7b' ),'hubid' ] = 'SG-04-hub00015'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af64' ),'hubid' ] = 'SG-04-hub00002'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af61' ),'hubid' ] = 'SG-04-hub00012'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af5b' ),'hubid' ] = 'SG-04-hub00008'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af2a' ),'hubid' ] = 'SG-04-hub00007'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af21' ),'hubid' ] = 'SG-04-hub00014'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af1f' ),'hubid' ] = 'SG-04-interdev'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af60' ),'hubid' ] = 'SG-04-starhub7'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af1e' ),'hubid' ] = 'SG-04-hub00019'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af6f' ),'hubid' ] = 'SG-04-hub00020'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af5e' ),'hubid' ] = 'SG-04-hub00021'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af47' ),'hubid' ] = 'SG-04-hub00017'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af5d' ),'hubid' ] = 'SG-04-hub00010'
    rms_raw.loc[(rms_raw['gwId'] == 'e47fb2f7af4e' ),'hubid' ] = 'SG-04-hub00009'

    return rms_raw

def del_neighbor(data,types):
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

def remove_outliers(table):
    i=1 ;n = table.shape[0]-1    
    while i<n:
        if table[i]>11:
            table[i]=max(table[i-1],table[i+1])
        i = i+1
    return table    

def get_agv_int(rms_data_input):
    rms_data_input['eventtime'] = rms_data_input['eventtime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))  
    intensity = rms_data_input.set_index('eventtime').groupby(pd.TimeGrouper(freq='5Min')).size()  
    intensity = list(filter(lambda x: x != 0,intensity))
    output = sum(intensity)/len(intensity)    
    return output

def time_picker(raw_wake_table,types,bathroom_table,rms_data_input,room_acttime_input):
    if types=='sleep':
        flag3 = setflag("01:30:01",day_now); flag4 = setflag("20:59:59",day_before)
        sleep = raw_wake_table[(raw_wake_table['time']<flag3)&(raw_wake_table['time']>flag4)]
        #if sleep.shape[0]>3:
            #sleep = sleep[sleep['gap']!=600]
        if sleep.shape[0]==0:
            sleep = sleep; print("no sleep data")
        if sleep.shape[0]==1:
            sleep = sleep
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
        flag3 = setflag("05:00:00",day_now)
        wakeup = raw_wake_table[raw_wake_table['time']>flag3]
#        if wakeup.shape[0]>3:
#            wakeup = wakeup[wakeup['gap']!=600]
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
        room_whole.index = range(room_whole.shape[0])
        return room_whole
    if types=='bedroom':
        if rawdata.shape[0]!=0:
            one_time_table = rawdata.groupby(pd.TimeGrouper('60s')).size() 
            five_time_table = rawdata.groupby(pd.TimeGrouper('300s')).size()    
            one_time_table = remove_outliers(one_time_table)
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
            awake_table = awake_table[(awake_table['sum']>12)|(awake_table['gap']<-5)]
            del awake_table['1min']; del awake_table['5min']
            awake_table.index = range(awake_table.shape[0])
        else:
            awake_table = pd.DataFrame()
        return awake_table

def time_normer(rawdata):
    rawdata = rawdata.dropna(subset=['starttime'],how='all')
    rawdata['time'] = rawdata['starttime'].apply(lambda x: str(x))
    rawdata['time'] = rawdata['time'].apply(lambda x: x[:19])
    rawdata['time'] = rawdata['time'].apply(lambda x: datetime.strptime(x,'%Y-%m-%dT%H:%M:%S')+timedelta(hours=8))
    rawdata['eventtime'] = rawdata['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')) 
    #flag3 = setflag("09:00:01",day_now); flag4 = setflag("19:59:59",day_before)
    return rawdata[(rawdata['time']<setflag("09:00:01",day_now))&(rawdata['time']>setflag("19:59:59",day_before))]


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
    
def check_awake_table(awake_table_input,raw_rms_input):
    '''
    This function is going to check whether the awake table is picking up all the morning and
     evening movement timestamp.
     if not, it will return timestamp following by the lone period blank
    '''
    flag2 = setflag("05:00:00",day_now)
    awake_table_test1 = awake_table_input[awake_table_input['time']>flag2]

    flag1=setflag("02:00:00",day_now)
    awake_table_test2 = awake_table_input[awake_table_input['time']<flag1]

    rawdata = raw_rms_input.set_index(raw_rms_input['eventtime'].map(parser.parse))
    rawdata['eventtime'] = rawdata['eventtime'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))

    if (awake_table_test1.shape[0]==0)&(awake_table_test2.shape[0]!=0):#wake up have not been capture
        rawdata = rawdata[rawdata['eventtime']>flag2]

    if (awake_table_test1.shape[0]!=0)&(awake_table_test2.shape[0]==0):#sleep have not been capture
        rawdata = rawdata[rawdata['eventtime']<flag1]

    if (awake_table_test1.shape[0]!=0)&(awake_table_test2.shape[0]!=0):#both have not been capture
        rawdata = pd.DataFrame()
    if rawdata.shape[0]!=0:
        five_time_table = rawdata.groupby(pd.TimeGrouper('300s')).size().to_frame()
        five_time_table = five_time_table.rename(index=str, columns={ 0: 'sum'})
        five_time_table['time'] = five_time_table.index
        five_time_table['time'] = five_time_table['time'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
        five_time_table.index = range(five_time_table.shape[0])
        first_line = five_time_table[:1]
        blank_time = five_time_table[five_time_table['sum']==0]
        blank_time['gap'] = blank_time['time'].diff()
        blank_time.index = range(blank_time.shape[0])
        last_line = five_time_table[-1:]

        if blank_time.shape[0]>2:
            blank_time['gap'].ix[0] = blank_time['time'][0]-first_line['time'][0]
            blank_time['gap'] = blank_time['gap'].apply(lambda x: x.seconds)
            i=0; n=blank_time.shape[0]-1
            add_frame = pd.DataFrame()
            while i<n:
                if (blank_time['gap'][i]==300)&(blank_time['gap'][i+1]==300):
                    add_line = blank_time.loc[[i]]
                    add_frame = add_frame.append(add_line)
                i=i+1
            #add_frame['time'] = add_frame['time'].apply(lambda x: x-timedelta(seconds=300))
            add_frame = add_frame.append(last_line)    

        if blank_time.shape[0]==2:
            add_frame = blank_time.loc[[1]]
            add_frame['time'] = add_frame['time'].apply(lambda x: x-timedelta(seconds=300))
            add_frame = add_frame.append(last_line)    

        if blank_time.shape[0]==1:
            add_frame = blank_time.loc[[0]]
            add_frame['time'] = add_frame['time'].apply(lambda x: x-timedelta(seconds=300))
            add_frame = add_frame.append(last_line)    
        if blank_time.shape[0]==0:
            add_frame = five_time_table[-1:]
            
        add_frame['gap'] = 20;add_frame['sum'] = 20    
        awake_table_input = awake_table_input.append(add_frame)
    if rawdata.shape[0]==0:
        awake_table_input = awake_table_input
    return awake_table_input



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

    if types =='wakeup':
        flag3 = setflag("05:00:00",day_now)
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
    return bathroom_time

    
def final_generator(hub_id,types,rms_whole_input,blob_df_whole_input):
    rms_data = rms_whole_input[rms_whole_input['hubid']==hub_id]
    blob_df = blob_df_whole_input[blob_df_whole_input['deviceid']==hub_id]
    awake_table = get_grouped(rms_data,'bedroom')
    if awake_table.shape[0]!=0:
        awake_table = check_awake_table(awake_table,rms_data)
    if blob_df.shape[0]!=0:     
        room_acttime = get_grouped(blob_df,'bathroom')
        print("multiroom user")
        if types=='wakeup':
            #awake_table = awake_table[awake_table['sum']>12]
            if awake_table.shape[0]>1:
                flag = awake_table['time'];flag.index = range(flag.shape[0])
                flag1 = flag[0]#;k = len(flag)-1;flag2 = flag[k]
                bathroom_time = bathroom_table_prep(room_acttime)
                bathroom_time = bathroom_time[bathroom_time['time']>flag1]
                awake_table = awake_table.append(bathroom_time)
                awake_table = del_neighbor(awake_table,'wakeup')
                awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
                wakeup = time_picker(awake_table,'wakeup',bathroom_time,rms_data,room_acttime)
            
            if awake_table.shape[0]==1:
                flag = awake_table['time'];flag.index = range(flag.shape[0])
                bathroom_time = bathroom_table_prep(room_acttime)
                flag1 = flag[0];flag2=setflag("05:00:00",day_now)
                if flag1<flag2:
                      bathroom_time = bathroom_time[bathroom_time['time']>flag1]
                if flag1>flag2:
                      flag3 = setflag("20:59:59",day_before)
                      bathroom_time = bathroom_time[bathroom_time['time']>flag3]
                awake_table = awake_table.append(bathroom_time)
                awake_table = del_neighbor(awake_table,'wakeup')      
                awake_table['time'] = awake_table['time'].apply(lambda x: x.to_datetime())
                wakeup = time_picker(awake_table,'wakeup',bathroom_time,rms_data,room_acttime)
            if awake_table.shape[0]==0:
                bathroom_time = bathroom_table_prep(room_acttime)
                if bathroom_time.shape[0]!=0:
                    bathroom_time = del_neighbor(bathroom_time,'wakeup')
                    wakeup = time_picker(bathroom_time,'wakeup',bathroom_time,rms_data,room_acttime)
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
                    sleep = time_picker(bathroom_time,'sleep',bathroom_time,rms_data,room_acttime)
                if bathroom_time.shape[0]==0:
                    sleep = pd.DataFrame(); sleep['wakeup'] = 'nan'
    if blob_df.shape[0]==0:
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
