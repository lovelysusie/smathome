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

wholeData$hubid =as.character(wholeData$hubid)

hublist = table(wholeData$hubid)
hublist = data.frame(hublist)
hublist$Var1 <-as.character(hublist$Var1)
hublist <-rxDataStep(inData = hublist, outFile = 'hublist.xdf')
hublist <-rxDataStep(inData = hublist)

partOne <- rxDataStep(inData = wholeData, 
                        rowSelection = (tasklocation == "Bedroom" &
                                         hubid==unique(hubid)[2]), 
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
########################
starting <- rxDataStep(inData = partOne, 
                       rowSelection = (status1 == 'start'), 
                       overwrite = TRUE)

ending <- rxDataStep(inData = partOne, 
                     rowSelection = (status2 == 'end'), 
                     overwrite = TRUE)

sleepAnalysis <-rxDataStep(inData = sleepAnalysis,overwrite = TRUE)

flag=sleepAnalysis$wakeup

starting <- rxDataStep(inData = starting, 
                       rowSelection = (starttime < strptime(paste(Sys.Date(),"07:30:01"), "%Y-%m-%d %H:%M:%S")), 
                       overwrite = TRUE)
ending <- rxDataStep(inData = ending, 
                     rowSelection = (starttime < strptime(paste(Sys.Date(),"07:30:01"), "%Y-%m-%d %H:%M:%S")), 
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
