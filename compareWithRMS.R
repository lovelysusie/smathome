library(dplyr)
library(lubridate)
library(ggplot2)
library(gridExtra)

adventis01 <-read.csv(file.choose())
adventis01 <-rbind(adventis01, read.csv(file.choose()))
adventis01$UTC...08.00 <-ymd_hms(adventis01$UTC...08.00)
head(adventis01[,1:10])
adventis01 <-adventis01[,1:10]

soundpress <-filter(adventis01, adventis01$Data.Type=="Sound intensity detection")
soundpress$Detail <-as.character(soundpress$Detail)
head(substr(soundpress$Detail, 37,38))
soundpress$Detail <-substr(soundpress$Detail, 37,38)
soundpress$Detail <-as.numeric(soundpress$Detail)

plot1 <-ggplot(data = soundpress, mapping = aes(x=UTC...08.00, y=Detail))+geom_line()

temperature <-filter(adventis01, adventis01$Data.Type=="Temp/humidity notice")
temperature$Detail <-as.character(temperature$Detail)
head(substr(temperature$Detail, 17,20),30)
temperature$Detail <-substr(temperature$Detail, 17,20)
names(temperature)[9] <-"temperature"
names(temperature)[10] <-"Humidity(%RH)"
temperature$`Humidity(%RH)` <-as.character(temperature$`Humidity(%RH)`)
head(substr(temperature$`Humidity(%RH)`, 16,20),30)
temperature$`Humidity(%RH)` <-substr(temperature$`Humidity(%RH)`, 16,20)
temperature$`Humidity(%RH)` <-as.numeric(temperature$`Humidity(%RH)`)
temperature$temperature <-as.numeric(temperature$temperature)

plot2 <-ggplot(data = temperature, mapping = aes(x=UTC...08.00, y=temperature))+geom_line()
plot3 <-ggplot(data = temperature, mapping = aes(x=UTC...08.00, y=`Humidity(%RH)`))+geom_line()

grid.arrange(plot1, plot2,plot3)

humanDetection <-filter(adventis01, adventis01$Data.Type=="Human detection")
humanDetection$Detail <-as.character(humanDetection$Detail)
humanDetection$Detail <-1
plot4 <-ggplot(data = humanDetection, aes(x=UTC...08.00, y=Detail))+geom_point()
grid.arrange(plot1, plot4)

adventisRaw <-read.csv(file.choose())
adventisRaw$EventProcessedUtcTime <-ymd_hms(adventisRaw$EventProcessedUtcTime)
adventisRaw <-filter(adventisRaw, adventisRaw$IoTHub.ConnectionDeviceId=="SG-04-avent001")
adventisRaw$date <-as.Date(adventisRaw$EventProcessedUtcTime)

day1 <-filter(adventisRaw, adventisRaw$date=="2017-01-01")
rawtemp <-filter(day1, day1$name=="temperature")
rawtemp$value <-as.character(rawtemp$value)
rawtemp$value <-as.numeric(rawtemp$value)
plot2 <-ggplot(data = temperature, mapping = aes(x=UTC...08.00, y=temperature))+geom_line()+
  geom_point(mapping=aes(x=rawtemp$EventProcessedUtcTime, y=rawtemp$value))
plot2

temperatureNew <-data.frame(time=temperature$UTC...08.00, temperature=temperature$temperature)
temperatureNew$from <-"RMS"
rawtemp <-filter(rawtemp, rawtemp$taskLocation=="Bedroom")
rawtemp <-data.frame(time=rawtemp$EventProcessedUtcTime, temperature=rawtemp$value)
rawtemp$from <-"PIR"

temperatureNew <-rbind(temperatureNew, rawtemp)
ggplot(temperatureNew, aes(x=time, y=temperature, color=from))+geom_point()+ggtitle("Jan01,temp")

library(plotrix)
twoord.plot(lx = temperature$UTC...08.00, ly = temperature$temperature, rx = temperature$UTC...08.00, 
            ry = temperature$`Humidity(%RH)`, type=c('point','point'),
            xtickpos=as.numeric(temperature$UTC...08.00), xticklab = as.character(temperature$UTC...08.00),
            lcol = 'steelblue', rcol = 'green', ylab = 'temperature', rylab = 'humidity',
            main = 'Jan 01')

test <-read.csv(file.choose())
test <-rbind(test, read.csv(file.choose()))
test$UTC...08.00 <-ymd_hms(test$UTC...08.00)
head(test[,1:10])
test <-test[,1:10]

adventis01 <-rbind(adventis01, test)
setwd("/Volumes/Untitled")
write.csv(adventis01, "RMS_adventis.csv", row.names = FALSE)

temperature$date<-as.Date(temperature$UTC...08.00)
temperatureNew <-filter(temperature, temperature$date=="2016-12-30")
temperatureNew <-data.frame(time=temperatureNew$UTC...08.00, temperature=temperatureNew$temperature)
temperatureNew$from <-"RMS"

day1 <-filter(adventisRaw, adventisRaw$date=="2016-12-30")
rawtemp <-filter(day1, day1$name=="temperature")
rawtemp$value <-as.character(rawtemp$value)
rawtemp$value <-as.numeric(rawtemp$value)
rawtemp <-filter(rawtemp, rawtemp$taskLocation=="Bedroom")
rawtemp <-data.frame(time=rawtemp$EventProcessedUtcTime, temperature=rawtemp$value)
rawtemp$from <-"PIR"

temperatureNew <-rbind(temperatureNew, rawtemp)
ggplot(temperatureNew, aes(x=time, y=temperature, color=from))+geom_point()+ggtitle("Dec30,temp")

setwd("/Volumes/Untitled")
#####################################
humanDetection <-adventis01
humanDetection$date <-as.Date(humanDetection$UTC...08.00)
humanDetection <-