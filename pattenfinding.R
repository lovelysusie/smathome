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
i=937
j=nrow(timetable)+1
while (i<j) {
  k=data.frame(bedroom=timetable$bedroom[i], bathroom=timetable$bathroom[i], 
               livingroom=timetable$livingroom[i],kitchen=timetable$kitchen[i])
  h=max(k)
  which(k=h,arr.ind = TRUE)
}