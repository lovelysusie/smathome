# import the data
inFile <- file.path(rxGetOption("sampleDataDir"), "RscaleTester.csv")

wholeData <- rxImport(inData=inFile, outFile = "hdbExample.xdf",
                      stringsAsFactors = TRUE, missingValueString = "M", rowsPerRead = 200000,
                      overwrite=TRUE)

rxGetInfo(wholeData, getVarInfo = TRUE)

# drop some variables
varsToDrop = c("name", "taskname","address","endtime")
hdfsFS <- RxHdfsFileSystem()
#########################
wholeData <- rxDataStep(inData = wholeData, 
                        varsToDrop = varsToDrop,
                        overwrite = TRUE)

hublist = table(wholeData$hubid)


wholeData <- rxDataStep(inData = wholeData, 
                        varsToDrop = varsToDrop,
                        rowSelection = (tasklocation == 'Bedroom' & 
                                          hubid == 'SG-04-avent001'), 
                        overwrite = TRUE)
wholeData$starttime = substr(wholeData$starttime,1,19)
wholeData$starttime = sub("T", " ",wholeData$starttime, fixed=FALSE)
wholeData$starttime <-strptime(wholeData$starttime, "%Y-%m-%d %H:%M:%S")
wholeData$starttime <-as.POSIXct(wholeData$starttime)
wholeData$date = as.Date(wholeData$starttime)

wholeData <- rxDataStep(inData = wholeData, 
                        rowSelection = (date == Sys.Date() | date == Sys.Date()-1), 
                        overwrite = TRUE)


#get the starting of the 30min interval
wholeData$status1 <-"duration"
i=1

while (i<nrow(wholeData)) {
  if (difftime(wholeData$starttime[i+1],wholeData$starttime[i], units = "min")>=30) wholeData$status1[i]="start" 
  i=i+1
}
###get the endin of the 30min interval
wholeData$status2 <-"duration"
i=1

while (i<nrow(wholeData)) {
  if (difftime(wholeData$starttime[i+1],wholeData$starttime[i], units = "min")>=30) wholeData$status2[i+1]="end" 
  i=i+1
}
##############################
sleepAnalysis <-data.frame(date = Sys.Date(), wakeup="07:30:01", sleep="21:59:59")
sleepAnalysis$wakeup <-paste(sleepAnalysis$date,sleepAnalysis$wakeup)  
sleepAnalysis$wakeup <-strptime(sleepAnalysis$wakeup, "%Y-%m-%d %H:%M:%S")
sleepAnalysis$wakeup <-as.POSIXct(sleepAnalysis$wakeup)

sleepAnalysis$sleep <-paste(sleepAnalysis$date,sleepAnalysis$sleep)  
sleepAnalysis$sleep <-strptime(sleepAnalysis$sleep, "%Y-%m-%d %H:%M:%S")
sleepAnalysis$sleep <-as.POSIXct(sleepAnalysis$sleep)

##############################
starting <- rxDataStep(inData = wholeData, 
                       rowSelection = (status1 == 'start'), 
                       overwrite = TRUE)

ending <- rxDataStep(inData = wholeData, 
                     rowSelection = (status2 == 'end'), 
                     overwrite = TRUE)

flag=sleepAnalysis$wakeup

starting <- rxDataStep(inData = starting, 
                       rowSelection = (starttime < flag), 
                       overwrite = TRUE)
ending <- rxDataStep(inData = ending, 
                     rowSelection = (starttime < sleepAnalysis$wakeup[1]), 
                     overwrite = TRUE)

sleepAnalysis$wakeup[1] <-tail(ending$time,1)

flag <-paste(sleepAnalysis$date[1], "05:00:00")
flag <-strptime(flag, "%Y-%m-%d %H:%M:%S")

if (nrow(starting)==0) sleepAnalysis$`wake up`[1] = head(series1$time,1)
if (sleepAnalysis$`wake up`[1] <flag) {
  series <- rxDataStep(inData = wholeData, 
                       rowSelection = (starttime > flag), 
                       overwrite = TRUE)
  sleepAnalysis$wakeup[1] = head(series$starttime,1)
}
