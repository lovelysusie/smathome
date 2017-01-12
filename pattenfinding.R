library(lubridate)
library(dplyr)
library(data.table)
library(sqldf)

hdbdata <-read.csv(file.choose())
hdbdata <-filter(hdbdata, hdbdata$IoTHub.ConnectionDeviceId=="SG-04-avent001")

hdbdata$time <-ymd_hms(hdbdata$EventProcessedUtcTime)
hdbdata$time <-hdbdata$time+28800
hdbdata$date <-as.Date(hdbdata$time)

hdbdata$time <-format(hdbdata$time, "%H:%M")

table(hdbdata$taskLocation)
calendar <-table(hdbdata$date)
calendar <-data.frame(calendar)
names(calendar) <-c("date","freq")
calendar$date <-as.Date(calendar$date)

#START
y=2
day1 <-filter(hdbdata, hdbdata$date==calendar$date[y])
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

#########
Store_Room <-filter(day1, day1$taskLocation=="Store Room"&day1$name=="movement")
if (nrow(Store_Room)==0) timeline$storeroom=NA

if (nrow(Store_Room)>0) {timetable <-table(Store_Room$time)
timetable <-data.frame(timetable)
names(timetable) <-c("time", "freq")

timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
timeline <-timeline[,-6]
names(timeline)[6] <-"storeroom"}

##########
outtime <-filter(day1, day1$taskName=="alert_button"&day1$name=="proximity")
timetable <-outtime[,c(17,15)]

timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
timeline <-timeline[,-7]
names(timeline)[7] <-"outtime"
###################################################
timetable <-timeline
timetable$bedroom[is.na(timetable$bedroom)] <-0
timetable$bathroom[is.na(timetable$bathroom)] <-0
timetable$livingroom[is.na(timetable$livingroom)] <-0
timetable$kitchen[is.na(timetable$kitchen)]<-0
timetable$storeroom[is.na(timetable$storeroom)]<-0

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
timetable <-timetable[,-7]
#######set the tasklocation column
i=1
j=nrow(timetable)+1
timetable$location = "pending"
while (i<j) {
  k=timetable[i,(2:5)]
  if (max(k)!=0) h=which.max(k) else h=0
  if (max(k)!=0 & h==1) timetable$location[i] = "bedroom" 
  if (max(k)!=0 & h==2) timetable$location[i] = "bathroom"
  if (max(k)!=0 & h==3) timetable$location[i] = "livingroom"
  if (max(k)!=0 & h==4) timetable$location[i] = "kitchen"
  if (max(k)!=0 & h==5) timetable$location[i] = "storeroom"
  i=i+1
}
#######set the status column
# k = which(timetable$location=="pending", arr.ind = TRUE)
timetable$status <-"lying"
timetable$status[timetable$location!="pending"]="awake"
#######
timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.timeline")

timeline <-timeline[,-c(8:13)]
timeline$location[is.na(timeline$location)] <-"outdoor"
timeline$status[is.na(timeline$status)] <-"outdoor activity"

#find out last location in each min
day1 <-filter(day1, day1$name=="movement")
day1$taskLocation=as.character(day1$taskLocation)
i=1
j=nrow(timeline)
while (i<j) {
  if (timeline$location[i]!="pending"&timeline$location[i+1]=="pending") {
    a=filter(day1, day1$time==timeline$timeline[i])
    a=tail(a,1)
    a=a$taskLocation
  timeline$location[i]=a}
  i=i+1
}

#starnderlize the locaiton
i=1 ;j=nrow(timeline)+1
while (i<j) {
  if (timeline$location[i]=="Kitchen") timeline$location[i]<-"kitchen"
  if (timeline$location[i]=="Bedroom") timeline$location[i]<-"bedroom"
  if (timeline$location[i]=="Living Room") timeline$location[i]<-"livingroom"
  if (timeline$location[i]=="Bathroom") timeline$location[i]<-"bathroom"
  i=i+1
}

timeline$location[timeline$location=="pending"] <-NA
replace_na_with_last<-function(x,a=!is.na(x)){
  x[which(a)[c(1,1:sum(a))][cumsum(a)+1]]
}
timeline$location <-replace_na_with_last(timeline$location)

timeline$status[timeline$location=="kitchen"&timeline$status=="lying"]="sitting"
timeline$status[timeline$location=="bathroom"&timeline$status=="lying"]="toilet"

##########################
timetable <-timeline[,c(1,8:9)]

#set out time as out
timetable$timegroup <-timetable$timeline
timetable$timegroup <-as.character(timetable$timegroup)
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
k=data.frame(k)
test <-data.frame(x1=k$Freq[1:48],x2=k$Freq[49:96])
k$Var2<-as.character(k$Var2)
names(test) <-c(k$Var2[1],k$Var2[49])
finaltable$awake=test$awake; finaltable$lying=test$lying
if (nrow(k)>143) {test$x3=k$Freq[97:144] ;names(test)[3]=k$Var2[97]}
if (nrow(k)>191) {test$x4=k$Freq[145:192] ;names(test)[4]=k$Var2[145]}
if (nrow(k)>239) {test$x5=k$Freq[193:240] ;names(test)[5]=k$Var2[193] }

if ("outdoor activity" %in% names(test)) finaltable$outdoor_activity=test$`outdoor activity` else finaltable$outdoor_activity=0
if ("sitting" %in% names(test)) finaltable$sitting=test$sitting else finaltable$sitting=0
if ("toilet" %in% names(test)) finaltable$toilet=test$toilet else finaltable$toilet=0


i=1
j=49
while (i<j) {
  h <-finaltable[i,2:6]
  if (which.max(h)==1) finaltable$status[i]="awake"
  if (which.max(h)==2) finaltable$status[i]="lying"
  if (which.max(h)==3) finaltable$status[i]="outdoor"
  if (which.max(h)==4) finaltable$status[i]="sitting"
  if (which.max(h)==5) finaltable$status[i]="toilet"
  i=i+1
}
finaltable <-finaltable[,-(2:6)]
#################################
#preparing for location
k=table(timetable$timegroup, timetable$location)
finaltable$bathroom = k[,1]
finaltable$bedroom = k[,2]
finaltable$kitchen = k[,3]
finaltable$livingoom = k[,4]
finaltable$outdoor = k[,5]


i=1
j=49
while (i<j) {
  h <-finaltable[i,3:7]
  if (which.max(h)==1) finaltable$location[i]="bathroom"
  if (which.max(h)==2) finaltable$location[i]="bedroom"
  if (which.max(h)==3) finaltable$location[i]="kitchen"
  if (which.max(h)==4) finaltable$location[i]="livingroom"
  if (which.max(h)==5) finaltable$location[i]="outdoor"
  i=i+1
}
#####################################
finaltable <-finaltable[,-c(3:7)]
finaltable$date <-calendar$date[y]
finaltable$week <-weekdays(finaltable$date)
finaltable1<-finaltable

###################################################################
#loop START
y=3
x=nrow(calendar)-1
while (y<x) {
  day1 <-filter(hdbdata, hdbdata$date==calendar$date[y])
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
  
  #########
  Store_Room <-filter(day1, day1$taskLocation=="Store Room"&day1$name=="movement")
  if (nrow(Store_Room)==0) timeline$storeroom=NA
  
  if (nrow(Store_Room)>0) {timetable <-table(Store_Room$time)
  timetable <-data.frame(timetable)
  names(timetable) <-c("time", "freq")
  
  timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
  timeline <-timeline[,-6]
  names(timeline)[6] <-"storeroom"}
  
  ##########
  outtime <-filter(day1, day1$taskName=="alert_button"&day1$name=="proximity")
  timetable <-outtime[,c(17,15)]
  
  timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.time")
  timeline <-timeline[,-7]
  names(timeline)[7] <-"outtime"
  ###################################################
  timetable <-timeline
  timetable$bedroom[is.na(timetable$bedroom)] <-0
  timetable$bathroom[is.na(timetable$bathroom)] <-0
  timetable$livingroom[is.na(timetable$livingroom)] <-0
  timetable$kitchen[is.na(timetable$kitchen)]<-0
  timetable$storeroom[is.na(timetable$storeroom)]<-0
  
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
  timetable <-timetable[,-7]
  #######set the tasklocation column
  i=1
  j=nrow(timetable)+1
  timetable$location = "pending"
  while (i<j) {
    k=timetable[i,(2:5)]
    if (max(k)!=0) h=which.max(k) else h=0
    if (max(k)!=0 & h==1) timetable$location[i] = "bedroom" 
    if (max(k)!=0 & h==2) timetable$location[i] = "bathroom"
    if (max(k)!=0 & h==3) timetable$location[i] = "livingroom"
    if (max(k)!=0 & h==4) timetable$location[i] = "kitchen"
    if (max(k)!=0 & h==5) timetable$location[i] = "storeroom"
    i=i+1
  }
  #######set the status column
  # k = which(timetable$location=="pending", arr.ind = TRUE)
  timetable$status <-"lying"
  timetable$status[timetable$location!="pending"]="awake"
  #######
  timeline <-sqldf("SELECT * FROM timeline LEFT OUTER JOIN timetable ON timeline.timeline=timetable.timeline")
  
  timeline <-timeline[,-c(8:13)]
  timeline$location[is.na(timeline$location)] <-"outdoor"
  timeline$status[is.na(timeline$status)] <-"outdoor activity"
  
  #find out last location in each min
  day1 <-filter(day1, day1$name=="movement")
  day1$taskLocation=as.character(day1$taskLocation)
  i=1
  j=nrow(timeline)
  while (i<j) {
    if (timeline$location[i]!="pending"&timeline$location[i+1]=="pending"&
        timeline$location[i]!="outdoor") {
      a=filter(day1, day1$time==timeline$timeline[i])
      a=tail(a,1)
      a=a$taskLocation
      timeline$location[i]=a}
    i=i+1
  }
  
  #starnderlize the locaiton
  i=1 ;j=nrow(timeline)+1
  while (i<j) {
    if (timeline$location[i]=="Kitchen") timeline$location[i]<-"kitchen"
    if (timeline$location[i]=="Bedroom") timeline$location[i]<-"bedroom"
    if (timeline$location[i]=="Living Room") timeline$location[i]<-"livingroom"
    if (timeline$location[i]=="Bathroom") timeline$location[i]<-"bathroom"
    i=i+1
  }
  
  timeline$location[timeline$location=="pending"] <-NA
  replace_na_with_last<-function(x,a=!is.na(x)){
    x[which(a)[c(1,1:sum(a))][cumsum(a)+1]]
  }
  timeline$location <-replace_na_with_last(timeline$location)
  
  timeline$status[timeline$location=="kitchen"&timeline$status=="lying"]="sitting"
  timeline$status[timeline$location=="bathroom"&timeline$status=="lying"]="toilet"
  
  ##########################
  timetable <-timeline[,c(1,8:9)]
  
  #set out time as out
  timetable$timegroup <-timetable$timeline
  timetable$timegroup <-as.character(timetable$timegroup)
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
  
  finaltable <-data.frame(table(timetable$timegroup, timetable$status))
  finaltable <-finaltable[1:48,1]
  finaltable <-data.frame(finaltable)
  k=table(timetable$timegroup, timetable$status)
  k=data.frame(k)
  test <-data.frame(x1=k$Freq[1:48],x2=k$Freq[49:96])
  k$Var2<-as.character(k$Var2)
  names(test) <-c(k$Var2[1],k$Var2[49])
  finaltable$awake=test$awake; finaltable$lying=test$lying
  if (nrow(k)>143) {test$x3=k$Freq[97:144] ;names(test)[3]=k$Var2[97]}
  if (nrow(k)>191) {test$x4=k$Freq[145:192] ;names(test)[4]=k$Var2[145]}
  if (nrow(k)>239) {test$x5=k$Freq[193:240] ;names(test)[5]=k$Var2[193] }
  
  if ("outdoor activity" %in% names(test)) finaltable$outdoor_activity=test$`outdoor activity` else finaltable$outdoor_activity=0
  if ("sitting" %in% names(test)) finaltable$sitting=test$sitting else finaltable$sitting=0
  if ("toilet" %in% names(test)) finaltable$toilet=test$toilet else finaltable$toilet=0
  
  i=1
  j=49
  while (i<j) {
    h <-finaltable[i,2:6]
    if (which.max(h)==1) finaltable$status[i]="awake"
    if (which.max(h)==2) finaltable$status[i]="lying"
    if (which.max(h)==3) finaltable$status[i]="outdoor"
    if (which.max(h)==4) finaltable$status[i]="sitting"
    if (which.max(h)==5) finaltable$status[i]="toilet"
    i=i+1
  }
  finaltable <-finaltable[,-c(2:6)]
  #################################
  #preparing for location
  k=table(timetable$timegroup, timetable$location)
  if (nrow(outtime)!=0){
  finaltable$bathroom = k[,1]
  finaltable$bedroom = k[,2]
  finaltable$kitchen = k[,3]
  finaltable$livingoom = k[,4]
  finaltable$outdoor = k[,5]
  
  i=1
  j=49
  while (i<j) {
    h <-finaltable[i,3:7]
    if (which.max(h)==1) finaltable$location[i]="bathroom"
    if (which.max(h)==2) finaltable$location[i]="bedroom"
    if (which.max(h)==3) finaltable$location[i]="kitchen"
    if (which.max(h)==4) finaltable$location[i]="livingroom"
    if (which.max(h)==5) finaltable$location[i]="outdoor"
    i=i+1
    }
  finaltable <-finaltable[,-c(3:7)]
  }
  
  if (nrow(outtime)==0){
    finaltable$bathroom = k[,1]
    finaltable$bedroom = k[,2]
    finaltable$kitchen = k[,3]
    finaltable$livingoom = k[,4]
    finaltable$outdoor=0
    
    i=1
    j=49
    while (i<j) {
      h <-finaltable[i,3:7]
      if (which.max(h)==1) finaltable$location[i]="bathroom"
      if (which.max(h)==2) finaltable$location[i]="bedroom"
      if (which.max(h)==3) finaltable$location[i]="kitchen"
      if (which.max(h)==4) finaltable$location[i]="livingroom"
      i=i+1
    }
    finaltable <-finaltable[,-c(3:7)]
  }
  
  
  #####################################
  
  finaltable$date <-calendar$date[y]
  finaltable$week <-weekdays(finaltable$date)
  finaltable1 <-rbind(finaltable1,finaltable)
  y=y+1
}








setwd("/Users/Susie/Desktop")
write.csv(finaltable1,"adv01_pattern.csv", row.names = TRUE)

#########################################################
library(arules)
library(arulesViz)

#patterns = random.patterns(nItems = 1000)
#summary(patterns)
#trans = random.transactions(nItems = 1000, nTrans = 1000, method = "agrawal",  patterns = patterns)
#image(trans)
finaltable <-finaltable1[,-4]
finaltable$status <-as.factor(finaltable$status)
finaltable$location <-as.factor(finaltable$location)
finaltable$week <-as.factor(finaltable$week)
finaltable <-finaltable[,-4]
rules <- apriori(finaltable,
                 parameter = list(minlen=2, supp=0.0002, conf=0.03),
                 appearance = list(rhs=c("finaltable=23:00"),
                                   default="lhs"),
                 control = list(verbose=F))
rules.sorted <- sort(rules, by="support")
inspect(rules.sorted)

###########################################################
x[which(a)[c(1,1:sum(a))][cumsum(a)+1]]


x =c(1,NA,NA,NA,3,4,5,NA,5,5,5,NA,NA,NA,6)
a
help("cumsum")
cumsum(a)
which(a)[c(1,1:sum(a))][cumsum(a)+1]
which(a)
