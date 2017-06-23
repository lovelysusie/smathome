# import the data
inFile <- file.path(rxGetOption("sampleDataDir"), "table0220.csv")

wholeData <- rxImport(inData=inFile, outFile = "hdb.xdf",
                stringsAsFactors = TRUE, missingValueString = "M", rowsPerRead = 200000,
                overwrite=TRUE)

rxGetInfo(wholeData, getVarInfo = TRUE)
outputpath <-tempfile(fileext = ".xdf")

# drop some variables
varsToDrop = c("name")
hdfsFS <- RxHdfsFileSystem()
#########################
wholeData <- rxDataStep(inData = wholeData, 
                        varsToDrop = varsToDrop,
                        overwrite = TRUE)

# as for adventis01
partOne <- rxDataStep(inData = wholeData, outFile = outputpath,
                        rowSelection = (tasklocation == "Bedroom" &
                                         hubid==unique(hubid)[2] & timegap > 25),
                        transforms = list(Rtime = as.POSIXct(starttime,"%Y-%m-%dT%H:%M:%S", tz="UTC")),
                        overwrite = TRUE)
wholeData <-rxSplit(inData = wholeData,splitByFactor="hubid",
                    transforms = list(Rtime = as.POSIXct(starttime,"%Y-%m-%dT%H:%M:%S", tz="UTC")),
                    overwrite = TRUE)
wholeData <-rxDataStep(inData = wholeData, outFile = partOne,
                       rowSelection = (starttime == starttime[1]),overwrite = TRUE)
head("hdb.hubid.SG-04-avent001.xdf",2)
test <-wholeData[3]
file <-RxXdfData(file = wholeData[1])
head(file,2)
#partOne$starttime = substr(partOne$starttime,1,19)
#partOne$starttime = sub("T", " ",partOne$starttime, fixed=FALSE)
#partOne$starttime <-strptime(partOne$starttime, "%Y-%m-%d %H:%M:%S")
#partOne$starttime <-as.POSIXct(partOne$starttime)
#partOne$date = as.Date(partOne$starttime)

rxDataStep(inData =  partOne,  outFile = outputpath,
           transforms = list(nexttime = lag(starttime,1,na.pad=TRUE),
                             date = as.Date(starttime)),
           overwrite = TRUE)
rxDataStep(outputpath,numRows = 5)


rxDataStep(inData = partOne, outFile = outputpath,
                      transforms = list(nexttime = as.Date(starttime)-1),
                      overwrite = TRUE)

hdfsFS2 <- RxHdfsFileSystem()
outputfile =RxXdfData(file = outputpath)

#get the starting of the 30min interval
partOne$status1 <-"duration"
i=1

while (i<nrow(partOne)) {
   if (difftime(partOne$starttime[i+1],partOne$starttime[i], units = "min")>=30) partOne$status1[i]="start" 
   i=i+1
}
###get the endin of the 30min interval
partOne$status2 <-"duration"
i=1

while (i<nrow(partOne)) {
   if (difftime(partOne$starttime[i+1],partOne$starttime[i], units = "min")>=30) partOne$status2[i+1]="end" 
   i=i+1
}
##############
#set up a sleep analysis chart
sleepAnalysis <-data.frame(date=rep(Sys.Date(),2), 
                           wakeup=rep(strptime(paste(Sys.Date(),"07:30:01"), "%Y-%m-%d %H:%M:%S"),2),
                           sleep=rep(strptime(paste(Sys.Date()-1,"20:59:59"), "%Y-%m-%d %H:%M:%S"),2))
###as for wake up time
starting <- rxDataStep(inData = partOne, 
                       rowSelection = (status1 == 'start'), 
                       overwrite = TRUE)

ending <- rxDataStep(inData = partOne, 
                     rowSelection = (status2 == 'end'), 
                     overwrite = TRUE)


starting <- rxDataStep(inData = starting, 
                       rowSelection = (starttime < strptime(paste(Sys.Date(),"07:30:01"), "%Y-%m-%d %H:%M:%S")), 
                       overwrite = TRUE)
ending <- rxDataStep(inData = ending, 
                     rowSelection = (starttime < strptime(paste(Sys.Date(),"07:30:01"), "%Y-%m-%d %H:%M:%S")), 
                     overwrite = TRUE)

sleepAnalysis$wakeup[1] <-tail(ending$starttime,1)

flag <-paste(sleepAnalysis$date[1], "05:00:00")
flag <-strptime(flag, "%Y-%m-%d %H:%M:%S")

partTwo <-rxDataStep(inData = partOne, 
                     rowSelection = (date == Sys.Date()), 
                     overwrite = TRUE)
if (nrow(ending)==0) sleepAnalysis$wakeup[1] = head(partTwo$time,1)
if (sleepAnalysis$wakeup[1] <flag) {
   series <- rxDataStep(inData = partOne, 
                       rowSelection = (starttime > strptime(paste(Sys.Date(),"05:00:00"), "%Y-%m-%d %H:%M:%S")), 
                       overwrite = TRUE)
   sleepAnalysis$wakeup[i] = head(series$starttime,1)
}

###as for sleep time
starting <- rxDataStep(inData = partOne, 
                       rowSelection = (status1 == 'start'), 
                       overwrite = TRUE)

ending <- rxDataStep(inData = partOne, 
                     rowSelection = (status2 == 'end'), 
                     overwrite = TRUE)

starting <- rxDataStep(inData = starting, 
                       rowSelection = (starttime > strptime(paste(Sys.Date()-1,"20:59:59"), "%Y-%m-%d %H:%M:%S")), 
                       overwrite = TRUE)
ending <- rxDataStep(inData = ending, 
                     rowSelection = (starttime > strptime(paste(Sys.Date()-1,"20:59:59"), "%Y-%m-%d %H:%M:%S")), 
                     overwrite = TRUE)

sleepAnalysis$sleep[1] <-head(starting$starttime,1)

partTwo <-rxDataStep(inData = partOne, 
                     rowSelection = (date == Sys.Date()-1), 
                     overwrite = TRUE)
if (nrow(ending)==0) sleepAnalysis$sleep[1] = tail(partTwo$time,1)
####################################################################################
# as for adventis02
partOne <- rxDataStep(inData = wholeData, 
                      rowSelection = (tasklocation == "Bedroom" &
                                        hubid==unique(hubid)[1]), 
                      overwrite = TRUE)

partOne$starttime = substr(partOne$starttime,1,19)
partOne$starttime = sub("T", " ",partOne$starttime, fixed=FALSE)
partOne$starttime <-strptime(partOne$starttime, "%Y-%m-%d %H:%M:%S")
partOne$starttime <-as.POSIXct(partOne$starttime)
partOne$date = as.Date(partOne$starttime)

partOne <- rxDataStep(inData = partOne, 
                      rowSelection = (date == Sys.Date()|date == Sys.Date()-1), 
                      overwrite = TRUE)


#get the starting of the 30min interval
partOne$status1 <-"duration"
i=1

while (i<nrow(partOne)) {
  if (difftime(partOne$starttime[i+1],partOne$starttime[i], units = "min")>=30) partOne$status1[i]="start" 
  i=i+1
}
###get the endin of the 30min interval
partOne$status2 <-"duration"
i=1

while (i<nrow(partOne)) {
  if (difftime(partOne$starttime[i+1],partOne$starttime[i], units = "min")>=30) partOne$status2[i+1]="end" 
  i=i+1
}

###as for wake up time
starting <- rxDataStep(inData = partOne, 
                       rowSelection = (status1 == 'start'), 
                       overwrite = TRUE)

ending <- rxDataStep(inData = partOne, 
                     rowSelection = (status2 == 'end'), 
                     overwrite = TRUE)


starting <- rxDataStep(inData = starting, 
                       rowSelection = (starttime < strptime(paste(Sys.Date(),"07:30:01"), "%Y-%m-%d %H:%M:%S")), 
                       overwrite = TRUE)
ending <- rxDataStep(inData = ending, 
                     rowSelection = (starttime < strptime(paste(Sys.Date(),"07:30:01"), "%Y-%m-%d %H:%M:%S")), 
                     overwrite = TRUE)

sleepAnalysis$wakeup[2] <-tail(ending$starttime,1)

flag <-paste(sleepAnalysis$date[1], "05:00:00")
flag <-strptime(flag, "%Y-%m-%d %H:%M:%S")

partTwo <-rxDataStep(inData = partOne, 
                     rowSelection = (date == Sys.Date()), 
                     overwrite = TRUE)
if (nrow(ending)==0) sleepAnalysis$wakeup[2] = head(partTwo$time,1)
if (sleepAnalysis$wakeup[2] <flag) {
  series <- rxDataStep(inData = partOne, 
                       rowSelection = (starttime > strptime(paste(Sys.Date(),"05:00:00"), "%Y-%m-%d %H:%M:%S")), 
                       overwrite = TRUE)
  sleepAnalysis$wakeup[i] = head(series$starttime,1)
}

###as for sleep time
starting <- rxDataStep(inData = partOne, 
                       rowSelection = (status1 == 'start'), 
                       overwrite = TRUE)

ending <- rxDataStep(inData = partOne, 
                     rowSelection = (status2 == 'end'), 
                     overwrite = TRUE)

starting <- rxDataStep(inData = starting, 
                       rowSelection = (starttime > strptime(paste(Sys.Date()-1,"20:59:59"), "%Y-%m-%d %H:%M:%S")), 
                       overwrite = TRUE)
ending <- rxDataStep(inData = ending, 
                     rowSelection = (starttime > strptime(paste(Sys.Date()-1,"20:59:59"), "%Y-%m-%d %H:%M:%S")), 
                     overwrite = TRUE)

sleepAnalysis$sleep[2] <-head(starting$starttime,1)

partTwo <-rxDataStep(inData = partOne, 
                     rowSelection = (date == Sys.Date()-1), 
                     overwrite = TRUE)
if (nrow(ending)==0) sleepAnalysis$sleep[2] = tail(partTwo$time,1)
sleepAnalysis$address=c("SG-04-avent001","SG-04-avent002")