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

day1 <-filter(hdbdata, hdbdata$date=="2016-11-24")
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

timeline$location[1:196] <-"bedroom"
timeline$location[202:302] <-"bedroom"
timeline$location[314:414] <-"bedroom"
#timeline$location[732] <-"bedroom"
timeline$location[1440] <-"bedroom"
timeline$location[1045:1051] <-"bedroom"
timeline$location[1053:1055] <-"bedroom"
timeline$location[1063:1078] <-"bedroom"
timeline$location[1134:1152] <-"bedroom"
timeline$location[1186:1187] <-"bedroom"
timeline$location[1189:1239] <-"bedroom"
timeline$location[1271:1274] <-"bedroom"
timeline$location[1282:1320] <-"bedroom"
timeline$location[1326:1436] <-"bedroom"



timeline$location[756:771] <-"livingroom"
timeline$location[780:785] <-"livingroom"
timeline$location[1153:1183] <-"livingroom"
timeline$location[1242:1269] <-"livingroom"

timeline$status[424:425] <-"toilet"
timeline$location[424:425] <-"bathroom"

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
k=table(timetable$timegroup, timetable$status)
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
#preparing for location
k=table(timetable$timegroup, timetable$location)
finaltable$bathroom = k[,1]
finaltable$bedroom = k[,2]
finaltable$kitchen = k[,3]
finaltable$livingoom = k[,4]
finaltable$outdoor = k[,5]
finaltable$pending = k[,6]

i=1
j=49
while (i<j) {
  h <-finaltable[i,7:12]
  if (which.max(h)==1) finaltable$location[i]="bathroom"
  if (which.max(h)==2) finaltable$location[i]="bedroom"
  if (which.max(h)==3) finaltable$location[i]="kitchen"
  if (which.max(h)==4) finaltable$location[i]="livingroom"
  if (which.max(h)==5) finaltable$location[i]="outdoor"
  if (which.max(h)==6) finaltable$location[i]="pending"
  i=i+1
}
#####################################
finaltable <-finaltable[,c(1,6,13)]
finaltable$week <-"Thursday"
finaltable$date <-"2016-11-24"
#####################################
finaltable1 <-finaltable
setwd("/Volumes/HAONAN/finaldata/")
write.csv(finaltable,"Nov24.csv", row.names = TRUE)
