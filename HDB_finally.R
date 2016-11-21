library(dplyr)
library(chron)
library(sqldf)
library(lubridate)
a <-read.csv(file.choose())


#time formate revision
a$time <-a$EventProcessedUtcTime
a$time <-substr(a$EventProcessedUtcTime, 1, 19)
a$time <-sub("T", " ",a$time, fixed=FALSE)
a$time <-strptime(a$time, "%Y-%m-%d %H:%M:%S")
a$time <-as.POSIXlt(a$time)
a$time <-a$time + 28800
head(a$time)
tail(a$time)

#get date
a$date <-a$time
a$date <-as.character(a$date)
a$date <-substr(a$date, 1, 10)
a$date <-as.Date(a$date)


##################
calender <-table(a$date)
calender <-data.frame(calender)
calender$Var1 <-as.Date(calender$Var1)
day <-filter(a, a$date==calender$Var1[1])

test1 <-calender$Var1[1]
test1 <-as.character(test1)
test1 <-paste(test1, "00:00:00")
test1 <-strptime(test1, "%Y-%m-%d %H:%M:%S")
test2 <-calender$Var1[1]
test2 <-as.character(test2)
test2 <-paste(test2, "23:55:00")
test2 <-strptime(test2, "%Y-%m-%d %H:%M:%S")
test <-seq(test1, test2, by = 300)
test[289] <-test[288]+300
test <-data.frame(test)
##################
d <-day$time

i=1
j=nrow(day)
while (i<289) {
  d[d>=test[i,1] & d<test[i+1,1]]=test[i,1]
  i=i+1
}

head(d,10)
tail(d,10)
d <-cbind(day,d)
d <-filter(d, d$name=="movement")
e <-table(d$d, d$taskLocation)

head(e,30)

f <-data.frame(e)

e <-filter(f, f$Var2=="Bathroom")
e$Var1 <-as.character(e$Var1)
test$test <-as.character(test$test)


newdata <-sqldf("SELECT * FROM test OUTER LEFT JOIN e ON test.test=e.Var1")
newdata <-newdata[,-(2:3)]
names(newdata)[2] <-"Bathroom"

e <-filter(f, f$Var2=="Bedroom")
e$Var1 <-as.character(e$Var1)
newdata <-sqldf("SELECT * FROM newdata OUTER LEFT JOIN e ON newdata.test=e.Var1")
newdata <-newdata[,-(3:4)]
names(newdata)[3] <-"Bedroom"

e <-filter(f, f$Var2=="Hall")
e$Var1 <-as.character(e$Var1)
newdata <-sqldf("SELECT * FROM newdata OUTER LEFT JOIN e ON newdata.test=e.Var1")
newdata <-newdata[,-(4:5)]
names(newdata)[4] <-"Hall"

e <-filter(f, f$Var2=="Kitchen")
e$Var1 <-as.character(e$Var1)
newdata <-sqldf("SELECT * FROM newdata OUTER LEFT JOIN e ON newdata.test=e.Var1")
newdata <-newdata[,-(5:6)]
names(newdata)[5] <-"Kitchen"


e <-filter(f, f$Var2=="Landing")
e$Var1 <-as.character(e$Var1)
newdata <-sqldf("SELECT * FROM newdata OUTER LEFT JOIN e ON newdata.test=e.Var1")
newdata <-newdata[,-(6:7)]
names(newdata)[6] <-"Landing"


e <-filter(f, f$Var2=="Living Room")
e$Var1 <-as.character(e$Var1)
newdata <-sqldf("SELECT * FROM newdata OUTER LEFT JOIN e ON newdata.test=e.Var1")
newdata <-newdata[,-(7:8)]
names(newdata)[7] <-"Living Room"

names(newdata)[1] <-"time"

newdata1 <-newdata


#######################

names(calender)[1] <-"date"

###########the second and the other day

j=2
k=nrow(calender)+1
while (j<k) {
  
  day <-filter(a, a$date==calender$date[j])
  
  test1 <-calender$date[j]
  test1 <-as.character(test1)
  test1 <-paste(test1, "00:00:00")
  test1 <-strptime(test1, "%Y-%m-%d %H:%M:%S")
  test2 <-calender$date[j]
  test2 <-as.character(test2)
  test2 <-paste(test2, "23:55:00")
  test2 <-strptime(test2, "%Y-%m-%d %H:%M:%S")
  test <-seq(test1, test2, by = 300)
  test[289] <-test[288]+300
  test <-data.frame(test)
  
  
  d <-day$time
  
  i=1
  while (i<289) {
    d[d>=test[i,1] & d<test[i+1,1]]=test[i,1]
    i=i+1
  }
  
  head(d,10)
  d <-cbind(day,d)
  d <-filter(d, d$name=="movement")
  e <-table(d$d, d$taskLocation)
  
  head(e,30)
  
  
  f <-data.frame(e)
  
  
  e <-filter(f, f$Var2=="Bathroom")
  e$Var1 <-as.character(e$Var1)
  test$test <-as.character(test$test)
  
  
  newdata <-sqldf("SELECT * FROM test OUTER LEFT JOIN e ON test.test=e.Var1")
  newdata <-newdata[,-(2:3)]
  names(newdata)[2] <-"Bathroom"
  
  e <-filter(f, f$Var2=="Bedroom")
  e$Var1 <-as.character(e$Var1)
  newdata <-sqldf("SELECT * FROM newdata OUTER LEFT JOIN e ON newdata.test=e.Var1")
  newdata <-newdata[,-(3:4)]
  names(newdata)[3] <-"Bedroom"
  
  e <-filter(f, f$Var2=="Hall")
  e$Var1 <-as.character(e$Var1)
  newdata <-sqldf("SELECT * FROM newdata OUTER LEFT JOIN e ON newdata.test=e.Var1")
  newdata <-newdata[,-(4:5)]
  names(newdata)[4] <-"Hall"
  
  e <-filter(f, f$Var2=="Kitchen")
  e$Var1 <-as.character(e$Var1)
  newdata <-sqldf("SELECT * FROM newdata OUTER LEFT JOIN e ON newdata.test=e.Var1")
  newdata <-newdata[,-(5:6)]
  names(newdata)[5] <-"Kitchen"
  
  
  e <-filter(f, f$Var2=="Landing")
  e$Var1 <-as.character(e$Var1)
  newdata <-sqldf("SELECT * FROM newdata OUTER LEFT JOIN e ON newdata.test=e.Var1")
  newdata <-newdata[,-(6:7)]
  names(newdata)[6] <-"Landing"
  
  
  e <-filter(f, f$Var2=="Living Room")
  e$Var1 <-as.character(e$Var1)
  newdata <-sqldf("SELECT * FROM newdata OUTER LEFT JOIN e ON newdata.test=e.Var1")
  newdata <-newdata[,-(7:8)]
  names(newdata)[7] <-"Living Room"
  
  names(newdata)[1] <-"time"
  
  newdata1 <-rbind(newdata1, newdata)
  
  j=j+1
  
}
#############################################
newdata <-filter(newdata1, newdata1$Bedroom!=0)
newdata$time <-strptime(newdata$time, "%Y-%m-%d %H:%M:%S")
###################


#######################################################################
#next step, find the gap which is larger than 30min interval
###################the first group
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
########################################
#####finding the real sleeping time
###after a general look of the dataframe newdata1, the lates getting up time is been seting as 8:30

#step 1, set 8:30 as a general wake up time

########################################
#####finding the real sleeping time
###after a general look of the dataframe newdata1, the lates getting up time is been seting as 8:30

#step 1, set 8:30 as a general wake up time

b <-calender

b$`wake up` <-paste(b$date, "07:30:01")
b$`wake up` <-strptime(b$`wake up`, "%Y-%m-%d %H:%M:%S")
b$`wake up` <-as.POSIXct(b$`wake up`)

b <-b[,-2]
############################################
newdata$time <-as.POSIXct(newdata$time)
###step2, find the real get up time

#getting the date
series <-newdata
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

flag <-paste(b$date[1], "05:00:00")
flag <-strptime(flag, "%Y-%m-%d %H:%M:%S")

if (nrow(starting)==0) b$`wake up`[1] = head(series1$time,1)
if (b$`wake up`[1] <flag) series1 <-filter(series1,series1$time>flag);b$`wake up`[1] = head(series1$time,1)
###

i=2
j=nrow(calender)+1
while (i <j){
  series1 <-filter(series, series$date==calender$date[i])
  starting <-filter(series1, series1$status1=="start")
  ending <-filter(series1, series1$status2=="end")
  
  
  starting <-filter(starting, starting$time<b$`wake up`[i] )
  ending <-filter(ending, ending$time<b$`wake up`[i])
  
  flag <-paste(b$date[i], "05:00:00")
  flag <-strptime(flag, "%Y-%m-%d %H:%M:%S")
  
  b$`wake up`[i] <-tail(ending$time,1)
  if (nrow(starting)==0) b$`wake up`[i] = head(series1$time,1)
  if (b$`wake up`[1] <flag) series1 <-filter(series1,series1$time>flag);b$`wake up`[i] = head(series1$time,1)
  i=i+1
}
###########################################
######################################################################################
######the sleeping time revise

#setting the sleeping time as 22:00 first.

b$sleep <-paste(b$date, "22:29:59")
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
###elemenate the NA of wakeup
i=1
while (i<nrow(calender)) {
  series1 <-filter(series, series$date==calender$date[i])
  if(nrow(series1)==0) b$`wake up`[i]=NA 
  if(nrow(series1)==0) b$sleep[i]=NA
  i=i+1
}

#################################################
i=2
series1 <-filter(series, series$date==calender$date[i]|series$date==calender$date[i-1])

starting <-filter(series1, series1$status1=="start")
ending <-filter(series1, series1$status2=="end")


ending <-filter(ending, ending$time>b$sleep[i-1] & ending$time<b$'wake up'[i])
starting <-filter(starting, starting$time>b$sleep[i-1] & starting$time <b$'wake up'[i])

night.up1<-rbind(head(ending,1),starting)

i=3
j=nrow(b)+1
while (i<j) {
  series1 <-filter(series, series$date==calender$date[i]|series$date==calender$date[i-1])
  
  starting <-filter(series1, series1$status1=="start")
  ending <-filter(series1, series1$status2=="end")
  
  
  ending <-filter(ending, ending$time>b$sleep[i-1] & ending$time<b$'wake up'[i])
  starting <-filter(starting, starting$time>b$sleep[i-1] & starting$time <b$'wake up'[i])
  night.up<-rbind(head(ending,1),starting)
  night.up1 <-rbind(night.up1, night.up)
  i=i+1
}

night.up1 <-night.up1[,-(2:9)]
###########
nightuptable <-data.frame(table(night.up1$date))
b$nightup<-0
nightuptable$Var1 <-as.Date(nightuptable$Var1)
names(nightuptable) <-c("date", "times")
help(match)

##################################################
setwd("/Users/Susie/Desktop")
write.csv(b, "HDB12_wakeup.csv", row.names = FALSE)
write.csv(night.up1, "HDB02_nightup.csv" , row.names = FALSE)
write.csv(nightuptable,"matching.csv", row.names = FALSE)
