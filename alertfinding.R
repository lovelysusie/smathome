library(lubridate)
library(dplyr)
library(data.table)
library(sqldf)

# normalize the date
hdbdata <-read.csv(file.choose())
hdbdata$time <-ymd_hms(hdbdata$eventprocessedutctime)
hdbdata$time <-hdbdata$time+28800
hdbdata$date <-as.Date(hdbdata$time)
#selsect the movement record
hdbdata <-filter(hdbdata, hdbdata$name=="movement")

#get the list of the date
calendar <-table(hdbdata$date)
calendar <-data.frame(calendar)
names(calendar) <-c("date","freq")
calendar$date <-as.Date(calendar$date)

#get the list of HDB
hdbNO <-table(hdbdata$connectiondeviceid)
hdbNO <-data.frame(hdbNO)
names(hdbNO) <-c("hdbNO","freq")

#select the exact date
day1 <-filter(hdbdata, hdbdata$date==calendar$date[2])
day1$tasklocation <-as.character(day1$tasklocation)
########################################
#start loop for each hdb
home <-filter(day1, day1$connectiondeviceid==hdbNO$hdbNO[2])
home$tasklocation <-as.character(home$tasklocation)
home$time <-format(home$time,"%H:%M")

timeline <-"2016-09-01 00:00:00"
timeline <-ymd_hms(timeline)
test <-"2016-09-01 23:59:00"
test <-ymd_hms(test)
timeline <-seq(timeline, test, by=60)

timeline <-format(timeline,"%H:%M")
timeline <-data.frame(timeline)

##############
bedroom <-filter(home, home$tasklocation=="Bedroom")
timetable <-table(bedroom$time)
timetable <-data.frame(timetable)
names(timetable) <-c("time", "freq")

timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
timeline <-timeline[,-2]
names(timeline)[2] <-"bedroom"
##############
bathroom <-filter(home, home$tasklocation=="Bathroom")
timetable <-table(bathroom$time)
timetable <-data.frame(timetable)
names(timetable) <-c("time", "freq")

timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
timeline <-timeline[,-3]
names(timeline)[3] <-"bathroom"
###############
livingroom <-filter(home, home$tasklocation=="Living Room")
timetable <-table(livingroom$time)
timetable <-data.frame(timetable)
names(timetable) <-c("time", "freq")

timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
timeline <-timeline[,-4]
names(timeline)[4] <-"livingroom"
#########
kitchen <-filter(home, home$tasklocation=="Kitchen")
timetable <-table(kitchen$time)
timetable <-data.frame(timetable)
names(timetable) <-c("time", "freq")

timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
timeline <-timeline[,-5]
names(timeline)[5] <-"kitchen"
#####################
#find out the abnormal line
timetable <-timeline
timetable$bedroom[is.na(timetable$bedroom)] <-0
timetable$bathroom[is.na(timetable$bathroom)] <-0
timetable$livingroom[is.na(timetable$livingroom)] <-0
timetable$kitchen[is.na(timetable$kitchen)]<-0

#setting up status
i=1
j=nrow(timetable)+1
while(i<j) {
  a=timetable$bedroom[i]+timetable$bathroom[i]+
    timetable$livingroom[i]+timetable$kitchen[i]
  if (a!=0) timetable$status[i]="normal" else timetable$status[i]="pending"
  i=i+1
}

#setting up location
i=1
j=nrow(timetable)+1
timetable$location <-"pending"
while (i<j) {
  k=timetable[i,(2:5)]
  if (max(k)!=0) h=which.max(k) else h=0
  if (max(k)!=0 & h==1) timetable$location[i] = "bathroom" 
  if (max(k)!=0 & h==2) timetable$location[i] = "bedroom"
  if (max(k)!=0 & h==3) timetable$location[i] = "livingroom"
  if (max(k)!=0 & h==4) timetable$location[i] = "kitchen"
  i=i+1
}

# find out the abnormal time
i=1
j=nrow(timetable)
while (i<j) {
  if (timetable$location[i]=="bathroom" & 
      timetable$status[i+1]=="pending") timetable$status[i]="abnormalStart"
  i=i+1
}

#find out the real abnormal time
i=1
j=nrow(timetable)
while (i<j) {
  if (timetable$status[i]=="abnormalStart") a=filter(home, home$time==timetable$timeline[i])
  if (tail(a,1)$tasklocation!="Bathroom") timetable$status[i]="normal"
  i=i+1
}

#find out the abnormal end time
a=1
i=1
while(i<nrow(timetable)) {
  if (timetable$status[i]=="abnormalStart") {
    j=i
    for (j in j:1440) {
      if (timetable$status[j]=="normal") break
      a = c(a,timetable$timeline[j])
    }
  }
  i=i+1
}

tail(a)
abnormalTime <-timetable$timeline[a]
abnormalTime <-abnormalTime[-1]
abnormalTime <-data.frame(abnormalTime)
abnormalTime$checking <-"abnormal"
timetable <-sqldf("SELECT * FROM timetable LEFT OUTER JOIN abnormalTime ON timetable.timeline=abnormalTime.abnormalTime")
timetable$checking[is.na(timetable$checking)] <-"normal"

i=1
j=nrow(timetable)
while (i<j) {
  if (timetable$checking[i]=="abnormal" & timetable$checking[i+1]=="normal") timetable$status[i]="abnormalEnd"
  i=i+1
}
##########
test <-which(timetable$status== "abnormalStart", arr.ind=TRUE)
test <-data.frame(start=test, starttime=timetable$timeline[test])
test$end <-which(timetable$status== "abnormalEnd", arr.ind=TRUE)
test$endtime <-timetable$timeline[test$end]
test$check=test$end-test$start
test=filter(test, test$check>14)
test$HDB <-hdbNO$hdbNO[2]
finally=test
#till here the first HBD is been collect
#################################################################
y=3
l=nrow(hdbNO)+1
while (k<l) {
  home <-filter(day1, day1$connectiondeviceid==hdbNO$hdbNO[y])
  home$tasklocation <-as.character(home$tasklocation)
  home$time <-format(home$time,"%H:%M")
  
  timeline <-"2016-09-01 00:00:00"
  timeline <-ymd_hms(timeline)
  test <-"2016-09-01 23:59:00"
  test <-ymd_hms(test)
  timeline <-seq(timeline, test, by=60)
  
  timeline <-format(timeline,"%H:%M")
  timeline <-data.frame(timeline)
  
  ##############
  bedroom <-filter(home, home$tasklocation=="Bedroom")
  timetable <-table(bedroom$time)
  timetable <-data.frame(timetable)
  names(timetable) <-c("time", "freq")
  
  timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
  timeline <-timeline[,-2]
  names(timeline)[2] <-"bedroom"
  ##############
  bathroom <-filter(home, home$tasklocation=="Bathroom")
  timetable <-table(bathroom$time)
  timetable <-data.frame(timetable)
  names(timetable) <-c("time", "freq")
  
  timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
  timeline <-timeline[,-3]
  names(timeline)[3] <-"bathroom"
  ###############
  livingroom <-filter(home, home$tasklocation=="Living Room")
  if (nrow(livingroom)>0) { 
  timetable <-table(livingroom$time)
  timetable <-data.frame(timetable)
  names(timetable) <-c("time", "freq")
  
  timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
  timeline <-timeline[,-4]
  names(timeline)[4] <-"livingroom"} else timeline$livingroom=NA
  #########
  kitchen <-filter(home, home$tasklocation=="Kitchen")
  if (nrow(kitchen)>0) {
  timetable <-table(kitchen$time)
  timetable <-data.frame(timetable)
  names(timetable) <-c("time", "freq") 
  
  timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
  timeline <-timeline[,-5]
  names(timeline)[5] <-"kitchen" } else timeline$kitchen=NA
  #####################
  #find out the abnormal line
  timetable <-timeline
  timetable$bedroom[is.na(timetable$bedroom)] <-0
  timetable$bathroom[is.na(timetable$bathroom)] <-0
  timetable$livingroom[is.na(timetable$livingroom)] <-0
  timetable$kitchen[is.na(timetable$kitchen)]<-0
  
  #setting up status
  i=1
  j=nrow(timetable)+1
  while(i<j) {
    a=timetable$bedroom[i]+timetable$bathroom[i]+
      timetable$livingroom[i]+timetable$kitchen[i]
    if (a!=0) timetable$status[i]="normal" else timetable$status[i]="pending"
    i=i+1
  }
  
  #setting up location
  i=1
  j=nrow(timetable)+1
  timetable$location <-"pending"
  while (i<j) {
    k=timetable[i,(2:5)]
    if (max(k)!=0) h=which.max(k) else h=0
    if (max(k)!=0 & h==1) timetable$location[i] = "bedroom" 
    if (max(k)!=0 & h==2) timetable$location[i] = "bathroom"
    if (max(k)!=0 & h==3) timetable$location[i] = "livingroom"
    if (max(k)!=0 & h==4) timetable$location[i] = "kitchen"
    i=i+1
  }
  
  # find out the abnormal time
  i=1
  j=nrow(timetable)
  while (i<j) {
    if (timetable$location[i]=="bathroom" & 
        timetable$status[i+1]=="pending") timetable$status[i]="abnormalStart"
    i=i+1
  }
  
  #find out the real abnormal time
  i=2
  j=nrow(timetable)+1
  while (i<j) {
    if (timetable$status[i]=="abnormalStart") {
      a=filter(home, home$time==timetable$timeline[i])
      a=tail(a,1)
        if (a$tasklocation!="Bathroom") timetable$status[i]="normal"}
    i=i+1
  }
  
  #find out the abnormal end time
  a=1
  i=1
  while(i<nrow(timetable)) {
    if (timetable$status[i]=="abnormalStart") {
      j=i+1
      for (j in j:1440) {
        if (timetable$status[j]=="normal") break
        a = c(a,timetable$timeline[j])
      }
    }
    i=i+1
  }
  
  tail(a)
  a=a[!duplicated(a)]
  b=which(timetable$status== "abnormalStart", arr.ind=TRUE)
  a=a[!a %in% b]
  abnormalTime <-timetable$timeline[a]
  abnormalTime <-abnormalTime[-1]
  abnormalTime <-data.frame(abnormalTime)
  abnormalTime$checking <-"abnormal"
  timetable <-sqldf("SELECT * FROM timetable LEFT OUTER JOIN abnormalTime ON timetable.timeline=abnormalTime.abnormalTime")
  timetable$checking[is.na(timetable$checking)] <-"normal"
  timetable <-timetable[!duplicated(timetable$timeline), ]
  
  timetable$status1 <-"normal"
  i=1
  j=nrow(timetable)
  while (i<j) {
    if (timetable$checking[i]=="abnormal" & timetable$checking[i+1]=="normal") timetable$status1[i]="abnormalEnd"
    i=i+1
  }
  ##########
  test <-which(timetable$status== "abnormalStart", arr.ind=TRUE)
  test <-data.frame(start=test, starttime=timetable$timeline[test])
  end=which(timetable$status1== "abnormalEnd", arr.ind=TRUE)
  
  i=nrow(test);j=length(end)
  if(i==j) {
  test$end <-which(timetable$status1== "abnormalEnd", arr.ind=TRUE)} 
  if(i!=j) {
    test=test[-nrow(test),]
    test$end <-which(timetable$status1== "abnormalEnd", arr.ind=TRUE)
  }
  test$endtime <-timetable$timeline[test$end]
  test$check=test$end-test$start
  test=filter(test, test$check>14)
  test$HDB <-hdbNO$hdbNO[y]
  finally <-rbind(finally,test)
  y=y+1
}