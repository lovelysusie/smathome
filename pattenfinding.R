library(dplyr)
library(lubridate)
library(dplyr)
library(data.table)
library(sqldf)

hdbdata <-read.csv(file.choose())
hdbdata <-filter(hdbdata, hdbdata$IoTHub=="{u'ConnectionDeviceId': u'SG-04-avent001', u'StreamId': None, u'MessageId': None, u'ConnectionDeviceGenerationId': u'636147895634468152', u'EnqueuedTime': u'0001-01-01T00:00:00.0000000', u'CorrelationId': None}")

hdbdata$time <-ymd_hms(hdbdata$EventProcessedUtcTime)
hdbdata$time <-hdbdata$time+28800
hdbdata$date <-as.Date(hdbdata$time)

hdbdata$time <-format(hdbdata$time, "%H:%M")

table(hdbdata$taskLocation)
calendar <-table(hdbdata$date)
calendar <-data.frame(calendar)
names(calendar) <-c("date","freq")

day1 <-filter(hdbdata, hdbdata$date=="2016-12-06")
bedroom <-filter(day1, day1$taskLocation=="Bedroom" & day1$name=="movement")
timetable <-table(bedroom$time)
timetable <-data.frame(timetable)
names(timetable) <-c("time", "freq")

timeline <-"2016-09-01 00:00:00"
timeline <-ymd_hms(timeline)
test <-"2016-09-01 23:59:00"
test <-ymd_hms(test)
timeline <-seq(timeline, test, by=60)
head(timeline)
timeline <-format(timeline,"%H:%M")
timeline <-data.frame(timeline)
timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
timeline <-timeline[,-2]
names(timeline)[2] <-"bedroom"

#########
bathroom <-filter(day1, day1$taskLocation=="Bathroom"&day1$name=="movement")
timetable <-table(bathroom$time)
timetable <-data.frame(timetable)
names(timetable) <-c("time", "freq")

timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
timeline <-timeline[,-3]
names(timeline)[3] <-"bathroom"

#########
livingroom <-filter(day1, day1$taskLocation=="Living Room"&day1$name=="movement")
timetable <-table(livingroom$time)
timetable <-data.frame(timetable)
names(timetable) <-c("time", "freq")

timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
timeline <-timeline[,-4]
names(timeline)[4] <-"livingroom"

#########
kitchen <-filter(day1, day1$taskLocation=="Kitchen"&day1$name=="movement")
timetable <-table(kitchen$time)
timetable <-data.frame(timetable)
names(timetable) <-c("time", "freq")

timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
timeline <-timeline[,-5]
names(timeline)[5] <-"kitchen"

##########
outtime <-filter(day1, day1$taskName=="alert_button"&day1$name=="proximity")
timetable <-outtime[,c(7,11)]

timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
timeline <-timeline[,-7]
names(timeline)[6] <-"outtime"
###################################################
timetable <-timeline
timetable$bedroom[is.na(timetable$bedroom)] <-0
timetable$bathroom[is.na(timetable$bathroom)] <-0
timetable$livingroom[is.na(timetable$livingroom)] <-0
timetable$kitchen[is.na(timetable$kitchen)]<-0
timetable$outtime[timetable$outtime=="false"] <-"out"
timetable$outtime[timetable$outtime=="true"] <-"in"
goingout <-which(timetable$outtime=="in", arr.ind=TRUE)
backin <-which(timetable$outtime=="out", arr.ind=TRUE)
#######delete the out door time
j=length(goingout)
while (j>0) {
  timetable <-timetable[-(goingout[j]:backin[j]),]
  j=j-1
}
timetable <-timetable[,-6]
#######set the tasklocation column
i=1
j=nrow(timetable)+1
while (i<j) {
  k=timetable[i,(2:5)]
  if (max(k)!=0) h=which.max(k) else timetable$location[i] = "pending"
  if (max(k)!=0 & h==1) timetable$location[i] = "bedroom" 
  if (max(k)!=0 & h==2) timetable$location[i] = "bathroom"
  if (max(k)!=0 & h==3) timetable$location[i] = "livingroom"
  if (max(k)!=0 & h==4) timetable$location[i] = "kitchen"
  i=i+1
}
#######set the status column
k = which(timetable$location=="pending", arr.ind = TRUE)
timetable$status <-"lying"
timetable$status[timetable$location!="pending"]="awake"
#######
timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.timeline")
timeline <-timeline[,-c(7:11)]
timeline$location[1:300] <-"bedroom"
timeline$location[306:393] <-"bedroom"
timeline$status[404] <-"toilet"
timeline$location[404] <-"bathroom"
timeline$location[485:498] <-"bedroom"
timeline$status[512] <-"toilet"
timeline$location[512] <-"bathroom"
timeline$location[815:816] <-"livingroom"
timeline$location[822:827] <-"livingroom"
timeline$location[837:838] <-"livingroom"
timeline$location[943:977] <-"livingroom"
timeline$location[985:986] <-"livingroom"

timeline$location[996:1022] <-"bedroom"
timeline$location[1025] <-"bedroom"
timeline$location[1240:1260] <-"bedroom"
timeline$location[1268:1440] <-"bedroom"

timeline$location[1104:1105] <-"livingroom"
timeline$location[1145:1181] <-"livingroom"
timeline$location[1196:1228] <-"livingroom"
##########################
timetable <-timeline[,c(1,7:8)]

#set out time as out
timetable$location[is.na(timetable$location)] <-"outdoor"
timetable$status[is.na(timetable$status)] <-"outdoor activity"
timetable$timegroup <-timetable$timeline
timetable$timegroup <-as.character(timetable$timegroup)
substr(timetable$timegroup[1:20], 4, 5)
i=1
j=nrow(timetable)+1
while (i<j) {
  if (substr(timetable$timegroup[i], 4, 4)=="0"|
      substr(timetable$timegroup[i], 4, 4)=="1"|
      substr(timetable$timegroup[i], 4, 4)=="2") substr(timetable$timegroup[i], 4, 5)="00"
  i=i+1
}

i=1
j=nrow(timetable)+1
while (i<j) {
  if (substr(timetable$timegroup[i], 4, 4)=="3"|
      substr(timetable$timegroup[i], 4, 4)=="4"|
      substr(timetable$timegroup[i], 4, 4)=="5") substr(timetable$timegroup[i], 4, 5)="30"
  i=i+1
}

finaltable <-data.frame(table(timetable$timegroup, timetable$status))
finaltable <-finaltable[1:48,1]
finaltable <-data.frame(finaltable)
finaltable$awake = k[,1]
finaltable$lying = k[,2]
finaltable$outdoor_activity = k[,3]
finaltable$toilet = k[,4]

i=1
j=49
while (i<j) {
  h <-finaltable[i,2:5]
  if (which.max(h)==1) finaltable$status[i]="awake"
  if (which.max(h)==2) finaltable$status[i]="lying"
  if (which.max(h)==3) finaltable$status[i]="outdoor"
  if (which.max(h)==4) finaltable$status[i]="toilet"
  i=i+1
}

i=1
j=49
while (i<j) {
  if (finaltable$toilet[i]>0) finaltable$status[i] = paste(finaltable$status[i],",toilet")
  i=i+1
}
#################################
