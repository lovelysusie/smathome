library(dplyr)
library(chron)
library(sqldf)
library(lubridate)
library(gridExtra)
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
calendar <-table(a$date)
calendar <-data.frame(calendar)
calendar$Var1 <-as.Date(calendar$Var1)
day <-filter(a, a$date==calendar$Var1[1])

test1 <-calendar$Var1[1]
test1 <-as.character(test1)
test1 <-paste(test1, "00:00:00")
test1 <-strptime(test1, "%Y-%m-%d %H:%M:%S")
test2 <-calendar$Var1[1]
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

names(calendar)[1] <-"date"

###########the second and the other day

j=2
k=nrow(calendar)+1
while (j<k) {
  
  day <-filter(a, a$date==calendar$date[j])
  
  test1 <-calendar$date[j]
  test1 <-as.character(test1)
  test1 <-paste(test1, "00:00:00")
  test1 <-strptime(test1, "%Y-%m-%d %H:%M:%S")
  test2 <-calendar$date[j]
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

b <-calendar

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
calendar$date <-as.Date(calendar$date)

#get the real get up time

series1 <-filter(series, series$date==calendar$date[1])

starting <-filter(series1, series1$status1=="start")
ending <-filter(series1, series1$status2=="end")

starting <-filter(starting, starting$time<b$`wake up`[1] )
ending <-filter(ending, ending$time<b$`wake up`[1])

b$`wake up`[1] <-tail(ending$time,1)

flag <-paste(b$date[1], "05:00:00")
flag <-strptime(flag, "%Y-%m-%d %H:%M:%S")

if (nrow(starting)==0) b$`wake up`[1] = head(series1$time,1)
if (b$`wake up`[1] <flag) series1 <-filter(series1,series1$time>flag)
if (b$`wake up`[1] <flag) b$`wake up`[1] = head(series1$time,1)
###

i=2
j=nrow(calendar)+1
while (i <j){
  series1 <-filter(series, series$date==calendar$date[i])
  starting <-filter(series1, series1$status1=="start")
  ending <-filter(series1, series1$status2=="end")
  
  
  starting <-filter(starting, starting$time<b$`wake up`[i] )
  ending <-filter(ending, ending$time<b$`wake up`[i])
  
  flag <-paste(b$date[i], "05:00:00")
  flag <-strptime(flag, "%Y-%m-%d %H:%M:%S")
  
  b$`wake up`[i] <-tail(ending$time,1)
  if (nrow(ending)==0) b$`wake up`[i] = head(series1$time,1)
  if (b$`wake up`[i] <flag) series1 <-filter(series1,series1$time>flag)
  if (b$`wake up`[i] <flag) b$`wake up`[i] = head(series1$time,1)
  i=i+1
}
###########################################
######################################################################################
######the sleeping time revise

#setting the sleeping time as 22:00 first.

b$sleep <-paste(b$date, "20:59:59")
b$sleep <-strptime(b$sleep, "%Y-%m-%d %H:%M:%S")
#find the real sleep time
i=2
j=nrow(calendar)+2
while (i <j){
  series1 <-filter(series, series$date==calendar$date[i]|series$date==calendar$date[i-1])
  
  
  starting <-filter(series1, series1$status1=="start")
  ending <-filter(series1, series1$status2=="end")
  
  
  ending <-filter(ending, ending$time>b$sleep[i-1] & ending$time<b$`wake up`[i])
  starting <-filter(starting, starting$time>b$sleep[i-1] & starting$time <b$`wake up`[i])
  
  b$sleep[i-1] <-head(starting$time,1)
  series1 <-filter(series, series$date==calendar$date[i-1])
  if (nrow(starting)==0) b$sleep[i-1] = tail(series1$time,1)
  i=i+1
}

##################################################
###elemenate the NA of wakeup
i=1
while (i<nrow(calendar)) {
  series1 <-filter(series, series$date==calendar$date[i])
  if(nrow(series1)==0) b$`wake up`[i]=NA 
  if(nrow(series1)==0) b$sleep[i]=NA
  i=i+1
}

test <-"2016-11-17 20:40:00"
test <-strptime(test, "%Y-%m-%d %H:%M:%S")
test <-as.POSIXlt(test)
b$sleep[2] <-test

#################################################
b$uptimes <-0

i=2
series1 <-filter(series, series$date==calendar$date[i]|series$date==calendar$date[i-1])

starting <-filter(series1, series1$status1=="start")
ending <-filter(series1, series1$status2=="end")


ending <-filter(ending, ending$time>b$sleep[i-1] & ending$time<b$'wake up'[i])
starting <-filter(starting, starting$time>b$sleep[i-1] & starting$time <b$'wake up'[i])

#night.up1<-rbind(head(ending,1),starting)
night.up1 <-ending
b$uptimes[i-1]<-nrow(ending)

i=3
j=nrow(b)+1
while (i<j) {
  series1 <-filter(series, series$date==calendar$date[i]|series$date==calendar$date[i-1])
  
  starting <-filter(series1, series1$status1=="start")
  ending <-filter(series1, series1$status2=="end")
  
  
  ending <-filter(ending, ending$time>b$sleep[i-1] & ending$time<b$'wake up'[i])
  starting <-filter(starting, starting$time>b$sleep[i-1] & starting$time <b$'wake up'[i])
  #night.up<-rbind(head(ending,1),starting)
  night.up <-ending
  night.up1 <-rbind(night.up1, night.up)
  b$uptimes[i-1]<-nrow(ending)
  i=i+1
}

night.up1 <-night.up1[,-(2:9)]

'''
i=1
j=nrow(night.up1)
while (i<j) {
  if (difftime(night.up1$time[i], night.up1$time[i+1], units("mins"))<30) {
    night.up1$time[i]=night.up1$time[i+1]
  }
  i=i+1
}
'''

table(night.up1$date)

###########

nightuptable <-data.frame(table(night.up1$date))
b$nightup<-0
nightuptable$Var1 <-as.Date(nightuptable$Var1)
names(nightuptable) <-c("date", "times")
help(match)

###########
#wakeup at night times
gg.gauge <- function(pos,breaks=c(0,20,40,60,80,100)) {
  require(ggplot2)
  get.poly <- function(a,b,r1=0.5,r2=1.0) {
    th.start <- pi*(1-a/100)
    th.end   <- pi*(1-b/100)
    th       <- seq(th.start,th.end,length=100)
    x        <- c(r1*cos(th),rev(r2*cos(th)))
    y        <- c(r1*sin(th),rev(r2*sin(th)))
    return(data.frame(x,y))
  }
  ggplot()+ 
    geom_polygon(data=get.poly(breaks[1],breaks[2]),aes(x,y),fill="olivedrab4")+
    geom_polygon(data=get.poly(breaks[2],breaks[3]),aes(x,y),fill="olivedrab3")+
    geom_polygon(data=get.poly(breaks[3],breaks[4]),aes(x,y),fill="olivedrab2")+
    geom_polygon(data=get.poly(breaks[4],breaks[5]),aes(x,y),fill="orange1")+
    geom_polygon(data=get.poly(breaks[5],breaks[6]),aes(x,y),fill="orangered")+
    geom_polygon(data=get.poly(pos,pos+1,0.2),aes(x,y))+
    geom_text(data=as.data.frame(breaks), size=6, fontface="bold", vjust=0,
              aes(x=1.1*cos(pi*(1-breaks/100)),y=1.1*sin(pi*(1-breaks/100)),label=c("0","1","2","3","4","5")))+
    annotate("text",x=0,y=0,label="2.47times",vjust=0,size=10,fontface="bold")+
    coord_fixed()+
    theme_bw()+
    theme(axis.text=element_blank(),
          axis.title=element_blank(),
          axis.ticks=element_blank(),
          panel.grid=element_blank(),
          panel.border=element_blank())+
    ggtitle("average frequency of wakeup at night(1106-0109)")+
    theme(plot.title = element_text(face="bold",size=17))
}
plot1<-gg.gauge(47,breaks=c(0,20,40,60,80,100))

###########
# calculate the sleeping time
sleepingHours <-data.frame(sleep=b$sleep[1:54], wakeup=b$`wake up`[2:55])
sleepingHours$hours <-difftime(sleepingHours$wakeup, sleepingHours$sleep, units = "hours")
  
gg.gauge <- function(pos,breaks=c(0,20,40,60,80,100)) {
  require(ggplot2)
  get.poly <- function(a,b,r1=0.5,r2=1.0) {
    th.start <- pi*(1-a/100)
    th.end   <- pi*(1-b/100)
    th       <- seq(th.start,th.end,length=100)
    x        <- c(r1*cos(th),rev(r2*cos(th)))
    y        <- c(r1*sin(th),rev(r2*sin(th)))
    return(data.frame(x,y))
  }
  ggplot()+ 
    geom_polygon(data=get.poly(breaks[1],breaks[2]),aes(x,y),fill="olivedrab4")+
    geom_polygon(data=get.poly(breaks[2],breaks[3]),aes(x,y),fill="olivedrab3")+
    geom_polygon(data=get.poly(breaks[3],breaks[4]),aes(x,y),fill="olivedrab2")+
    geom_polygon(data=get.poly(breaks[4],breaks[5]),aes(x,y),fill="orange1")+
    geom_polygon(data=get.poly(breaks[5],breaks[6]),aes(x,y),fill="orangered")+
    geom_polygon(data=get.poly(pos,pos+1,0.2),aes(x,y))+
    geom_text(data=as.data.frame(breaks), size=6, fontface="bold", vjust=0,
              aes(x=1.1*cos(pi*(1-breaks/100)),y=1.1*sin(pi*(1-breaks/100)),label=c("10h","9h","8h","7h","6h","5h")))+
    annotate("text",x=0,y=0,label="8hrs45min",vjust=0,size=8,fontface="bold")+
    coord_fixed()+
    theme_bw()+
    theme(axis.text=element_blank(),
          axis.title=element_blank(),
          axis.ticks=element_blank(),
          panel.grid=element_blank(),
          panel.border=element_blank())+
    ggtitle("average sleeping time(1106-0109)")+
    theme(plot.title = element_text(face="bold",size=17))
}
plot2 <-gg.gauge(35,breaks=c(0,20,40,60,80,100))


##################################################
setwd("/Users/Susie/Desktop")
write.csv(b, "Adventis_wakeup_Jan09.csv", row.names = FALSE)
write.csv(night.up1, "Adventis_nightup.csv" , row.names = FALSE)
write.csv(nightuptable,"matching.csv", row.names = FALSE)
