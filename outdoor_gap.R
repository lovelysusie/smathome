library(dplyr)

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


setwd("/Users/Susie/Desktop")
write.csv(HDB.No, "hdb_ourdoor_freq.csv", row.names = FALSE)
