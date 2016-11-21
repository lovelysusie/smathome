##################################################
###elemenate the NA of calendar,delete the day without bedroom movement
series <-newdata
series$date <-as.character(series$time)
series$date <-substr(series$date, 1,10)
series$date <-as.Date(series$date)
calender$date <-as.Date(calender$date)


i=1
while (i<nrow(calender)) {
  series1 <-filter(series, series$date==calender$date[i])
  if(nrow(series1)==0) calender$Freq[i]=NA 
  i=i+1
}

calender<-na.omit(calender)
#######################################################################
##############next step, find the gap which is larger than 30min interval

newdata <-filter(newdata1, newdata1$Bedroom!=0)
newdata$time <-strptime(newdata$time, "%Y-%m-%d %H:%M:%S")

#get the starting of the 30min interval

newdata$status1 <-"duration"
i=1

while (i<nrow(newdata)) {
  if (difftime(newdata$time[i+1],newdata$time[i], units = "min")>=30) newdata$status1[i]="start" 
  i=i+1
}
###get the endin of the 30min interval
newdata$status2 <-"duration"
i=1

while (i<nrow(newdata)) {
  if (difftime(newdata$time[i+1],newdata$time[i], units = "min")>=30) newdata$status2[i+1]="end" 
  i=i+1
}
###########################################
newdata1$time <-strptime(newdata1$time, "%Y-%m-%d %H:%M:%S")
newdata1$time <-as.POSIXct(newdata1$time)
newdata$time <-as.POSIXct(newdata$time)
####add the status column
newdata1 <-sqldf("SELECT * FROM newdata1 OUTER LEFT JOIN newdata ON newdata.time=newdata1.time")
newdata1 <-newdata1[,-(8:14)]
####dealing with the missing value
newdata1$status1[is.na(newdata1$status1)] <-"duration"
newdata1$status2[is.na(newdata1$status2)] <-"duration"
#################################################################################
##dealing with newdata1(newdata1 is the 5 min data sheet)
del <-seq(289,nrow(newdata1), by=289)
del <-del[-44]
tail(del)
newdata1$time[12427]
newdata1 <-newdata1[-del,]
######################################################################
newdata1$Bathroom[is.na(newdata1$Bathroom)] <-0
newdata1$Bedroom[is.na(newdata1$Bedroom)] <-0
newdata1$Hall[is.na(newdata1$Hall)] <-0
newdata1$Kitchen[is.na(newdata1$Kitchen)] <-0
newdata1$Landing[is.na(newdata1$Landing)] <-0
newdata1$`Living Room`[is.na(newdata1$`Living Room`)] <-0
#######getting the date
newdata1$date <-as.character(newdata1$time)
newdata1$date <-substr(newdata1$date, 1,10)
newdata1$date <-as.Date(newdata1$date)

###########################
newdata1$status3 <-"pending"
i=1
j=nrow(newdata1)
while (i<j) {
  if (
    (newdata1$Bathroom[i]!=0 | newdata1$Bedroom[i]!=0 | newdata1$Hall[i]!=0 |
     newdata1$Kitchen[i]!=0 |newdata1$Landing[i]!=0 |newdata1$`Living Room`[i]!=0) &
    (newdata1$Bathroom[i+1]==0 & newdata1$Bedroom[i+1]==0 & newdata1$Hall[i+1]==0 &
     newdata1$Kitchen[i+1]==0 &newdata1$Landing[i+1]==0 & newdata1$`Living Room`[i+1]==0) 
  ) newdata1$status3[i+1]="gap start"
  i=i+1
}

###########################
newdata1$status4 <-"pending"
i=1
j=nrow(newdata1)
while (i<j) {
  if (
    (newdata1$Bathroom[i+1]!=0 | newdata1$Bedroom[i+1]!=0 | newdata1$Hall[i+1]!=0 |
     newdata1$Kitchen[i+1]!=0 |newdata1$Landing[i+1]!=0 |newdata1$`Living Room`[i+1]!=0) &
    (newdata1$Bathroom[i]==0 & newdata1$Bedroom[i]==0 & newdata1$Hall[i]==0 &
     newdata1$Kitchen[i]==0 &newdata1$Landing[i]==0 & newdata1$`Living Room`[i]==0) 
  ) newdata1$status4[i]="gap end"
  i=i+1
}
###########################
gap <-filter(newdata1, newdata1$status3=="gap start"|newdata1$status4=="gap end")
gap <-gap[,-(2:9)]

starting <-filter(gap, gap$status3=="gap start")
starting <-starting[-nrow(starting),]
ending <-filter(gap, gap$status4=="gap end")
ending <-ending[-1,]
gap <-data.frame(start=starting$time, end=ending$time, gap=ending$time-starting$time+300)
gap$end <-gap$end+300
large_gap <-filter(gap, gap$gap>1800)
large_gap$HRS <-difftime(large_gap$end, large_gap$start, units = "hours")
large_gap$HRS <-round(large_gap$HRS,2)

i=1
j=nrow(large_gap)+1
while (i<j) {
  k = which(newdata1$time==large_gap$start[i], arr.ind = FALSE)
  newdata1$status3[k]="starting"
  i=i+1
}

i=1
j=nrow(large_gap)+1
while (i<j) {
  k = which(newdata1$time==large_gap$end[i], arr.ind = FALSE)
  newdata1$status4[k]="ending"
  i=i+1
}

newdata1$status1[newdata1$status1!="start"] <-" "
newdata1$status2[newdata1$status2!="end"] <-" "
newdata1$status3[newdata1$status3!="starting"] <-" "
newdata1$status4[newdata1$status4!="ending"] <-" "
#################################################################################

###########################################
b <-calender

b$`wake up` <-paste(b$date, "07:30:01")
b$`wake up` <-strptime(b$`wake up`, "%Y-%m-%d %H:%M:%S")
b$`wake up` <-as.POSIXct(b$`wake up`)
b <-b[,-2]
############################################
newdata$time <-as.POSIXct(newdata$time)
###step2, find the real get up time

#getting the date
series <-newdata1
series$date <-as.character(series$time)
series$date <-substr(series$date, 1,10)
series$date <-as.Date(series$date)
calender$date <-as.Date(calender$date)

#get the real get up time

series1 <-filter(series, series$date==calender$date[1])

starting <-filter(series1, series1$status1=="start")
ending <-filter(series1, series1$status2=="end")

starting <-filter(starting, starting$time<b$`wake up`[1] )
ending <-filter(ending, ending$time<b$`wake up`[1])

b$`wake up`[1] <-tail(ending$time,1)

flag <-paste(b$date[1], "04:59:59")
flag <-strptime(flag, "%Y-%m-%d %H:%M:%S")
flag <-as.POSIXct(flag)


if (nrow(ending)==0) b$`wake up`[1] = head(filter(series1,series1$Bedroom!=0)$time,1)
#if (b$`wake up`[1] <flag) series1 <-filter(series1,series1$time>flag) 
#if (b$`wake up`[1] <flag) b$`wake up`[1] = head(series1$time,1)

k=which(newdata1$time==b$`wake up`[1], arr.ind = TRUE)
flag1 <-paste(b$date[1], "07:30:01")
flag1 <-strptime(flag1,"%Y-%m-%d %H:%M:%S")
flag1 <-as.POSIXct(flag1)
flag2 <-paste(b$date[1], "10:30:01")
flag2 <-strptime(flag2,"%Y-%m-%d %H:%M:%S")
flag2 <-as.POSIXct(flag2)

series1 <-filter(series, series$time>newdata1$time[k]&series$time<flag1)
if ("starting" %in% series1$status3) series2<-filter(series, series$time>flag1 & series$time<flag2) 
if ("starting" %in% series1$status3) ending <-filter(series2, series2$status2=="end") 
if ("starting" %in% series1$status3) b$`wake up`[1]<-head(ending$time,1)
  
#############################################################################################
i=2
j=nrow(calender)+1
while (i <j){
  series1 <-filter(series, series$date==calender$date[i])
  starting <-filter(series1, series1$status1=="start")
  ending <-filter(series1, series1$status2=="end")
  
  
  starting <-filter(starting, starting$time<b$`wake up`[i] )
  ending <-filter(ending, ending$time<b$`wake up`[i])
  
  b$`wake up`[i] <-tail(ending$time,1)
  
  flag <-paste(b$date[i], "04:59:59")
  flag <-strptime(flag, "%Y-%m-%d %H:%M:%S")
  flag <-as.POSIXct(flag)
  
  
  if (nrow(ending)==0) b$`wake up`[i] = head(filter(series1,series1$Bedroom!=0)$time,1)
  #if (b$`wake up`[i] <flag) series1 <-filter(series1,series1$time>flag)  
  #if (b$`wake up`[i] <flag) b$`wake up`[i] = head(series1$time,1)
    
  k=which(newdata1$time==b$`wake up`[i], arr.ind = TRUE)
  flag1 <-paste(b$date[i], "07:30:01")
  flag1 <-strptime(flag1,"%Y-%m-%d %H:%M:%S")
  flag1 <-as.POSIXct(flag1)
  flag2 <-paste(b$date[i], "10:30:01")
  flag2 <-strptime(flag2,"%Y-%m-%d %H:%M:%S")
  flag2 <-as.POSIXct(flag2)
  
  series1 <-filter(series, series$time>newdata1$time[k]&series$time<flag1)
  if ("starting" %in% series1$status3) series2<-filter(series, series$time>flag1 & series$time<flag2) 
  if ("starting" %in% series1$status3) ending <-filter(series2, series2$status2=="end") 
  if ("starting" %in% series1$status3) b$`wake up`[i]<-head(ending$time,1)
  i=i+1
}
######the sleeping time revise

#setting the sleeping time as 22:00 first.

b$sleep <-paste(b$date, "21:59:59")
b$sleep <-strptime(b$sleep, "%Y-%m-%d %H:%M:%S")
#find the real sleep time
i=2
j=nrow(calender)+2
while (i <j){
  series1 <-filter(series, series$date==calender$date[i]|series$date==calender$date[i-1])
  
  
  starting <-filter(series1, series1$status1=="start")
  ending <-filter(series1, series1$status2=="end")
  
  
  ending <-filter(ending, ending$time>b$sleep[i-1] & ending$time<b$`wake up`[i])
  starting <-filter(starting, starting$time>b$sleep[i-1] & starting$time <b$`wake up`[i])
  
  b$sleep[i-1] <-head(starting$time,1)
  series1 <-filter(series, series$date==calender$date[i-1])
  if (nrow(starting)==0) b$sleep[i-1] = tail(series1$time,1)
  i=i+1
}
##################################################
########revising
b$`wake up`[38]=NA
b$sleep[38]=NA
flag =strptime("2016-11-06 10:15:00","%Y-%m-%d %H:%M:%S")
b$`wake up`[37]=flag
b1 <-b
###################################################
write.csv(b, "HDB02_wakeup.csv", row.names = FALSE)

