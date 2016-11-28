library(dplyr)
library(timevis)

hdb <-read.csv(file.choose())
a <-hdb

a$time <-a$EventProcessedUtcTime
a$time <-substr(a$EventProcessedUtcTime, 1, 19)
a$time <-sub("T", " ",a$time, fixed=FALSE)
a$time <-strptime(a$time, "%Y-%m-%d %H:%M:%S")
a$time <-as.POSIXlt(a$time)
a$time <-a$time + 28800
head(a$time)
tail(a$time)

hdb <-a
a <-NULL

keys <-filter(hdb, hdb$taskName=="alert_button")
HDB.No <-table(keys$IoTHub)
HDB.No <-data.frame(HDB.No)
names(HDB.No) <-c("HDB","outdoor times")
HDB.No$`outdoor times` <-HDB.No$`outdoor times`/2
HDB.No$`outdoor times` <-round(HDB.No$`outdoor times`,0)

hdb02 <-filter(keys, keys$IoTHub=="SG-04-HDB00002")

out <-filter(hdb02, hdb02$value=="false")
backin <-filter(hdb02, hdb02$value=="true")
outtime <-data.frame(out=out$time, backin=backin$time)
outtime$duration <-difftime(outtime$backin,outtime$out, units="mins")


hdb03 <-filter(keys, keys$IoTHub=="SG-04-HDB00003")

out <-filter(hdb03, hdb03$value=="false")
backin <-filter(hdb03, hdb03$value=="true")
outtime <-data.frame(out=out$time, backin=backin$time)
outtime$duration <-difftime(outtime$backin,outtime$out, units="mins")
##################################
#for advent rooms
out <-filter(keys, keys$value=="FALSE")
backin <-filter(keys, keys$value=="TRUE")
outtime <-data.frame(out=out$time[1:12], backin=backin$time[2:13])
outtime$duration <-difftime(outtime$backin,outtime$out, units="mins")
#draw the outdoor graph
outtime$date <-as.character(outtime$out)
outtime$date <-substr(outtime$date,6,10)

outtime$out <-as.character(outtime$out)
substr(outtime$out,9,10) <-"16"
outtime$out <-strptime(outtime$out, "%Y-%m-%d %H:%M:%S")
outtime$out <-as.POSIXct(outtime$out)

outtime$backin <-as.character(outtime$backin)
substr(outtime$backin,9,10) <-"16"
outtime$backin <-strptime(outtime$backin, "%Y-%m-%d %H:%M:%S")
outtime$backin <-as.POSIXct(outtime$backin)

####
data <- data.frame(
  id      = 1:nrow(outtime),
  content = outtime$date,
  start   = outtime$out,
  end     = outtime$backin
)
timevis(data)

##########
outtime$out <-as.character()
##########
setwd("/Users/Susie/Desktop")
write.csv(outtime, "ourdoor_gap.csv", row.names = FALSE)
