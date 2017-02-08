# accept the arguments from command line
args = (commandArgs(TRUE))

if (length(args) < 2) {
    stop ("At least 2 argument must be supplied. 1st for the location of the 
        dataset. 2nd is the directory on hdfs for the output model storage", call.=FALSE) 
}

# print the arguments for debugging purposes
# Set the spark context and define the executor memory and processor cores. 



  outputFile2 <- args[2]

  inputFile2  <- args[1]
  #name of the final model object
  #tempLocalModelFile <- 'testadfmodel'
  filePrefix <- "/tmp"
  rxSetComputeContext(RxSpark(hdfsShareDir = "/tmp", consoleOutput=TRUE, executorMem="2g", executorCores = 2, driverMem="1g", executorOverheadMem="1g") )
  hdfsFS2 <- RxHdfsFileSystem()

  
xdfOutFile2 <- file.path(filePrefix, "testadfxdf2")
testDataSplitXdfFile2 <- file.path(filePrefix, "testadfSplitXdf2")

testDataClasses2 <- c(starttime = "character",endtime = "character",
                        hubid = "character",address = "character", 
                        tasklocation = "character",
                        name = "character", 
                        taskname = "character",
                        value = "numeric" )


testDataDS2 <- RxTextData(file = inputFile2, fileSystem = hdfsFS2, delimiter = ",", firstRowIsColNames = TRUE,
                     colClasses = testDataClasses2)



xdfOut2 <- RxXdfData(file = xdfOutFile2, fileSystem = hdfsFS2)



testDataDSXdf2 <- rxImport(inData = testDataDS2, outFile = xdfOut2,
                      createCompositeSet = TRUE,
                      overwrite = TRUE)

rxGetInfo( data = testDataDSXdf2, getVarInfo = TRUE)

#set the path of output
testDataSplitXdf2 <- RxXdfData(file = testDataSplitXdfFile2, fileSystem = hdfsFS2);



# drop some variables
varsToDrop = c("name", "taskname","address","endtime")

# rxDataStep(inData = taxiDSXdf, outFile = taxiSplitXdf,
#            varsToDrop = varsToDrop,
#            rowSelection = (passenger_count > 0 & passenger_count < 8 &
#                              tip_amount >= 0 & tip_amount <= 40 &
#                              fare_amount > 0 & fare_amount <= 200 &
#                              trip_distance > 0 & trip_distance <= 100 &
#                              trip_time_in_secs > 10 & trip_time_in_secs <= 7200),
#            overwrite = TRUE)

# outFile的设定在这里在这里

testDataDSXdf2 <-rxDataStep(inData = testDataDSXdf2, 
           varsToDrop = varsToDrop,
           rowSelection = (tasklocation == 'Bedroom'),
           overwrite = TRUE)
# 等一下回来再看看这里

# as for adventis01
partOne <- rxDataStep(inData = testDataDSXdf2, 
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
partOne <- rxDataStep(inData = testDataDSXdf2, 
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

rxDataStep(inData = sleepAnalysis, outFile = testDataSplitXdf2, overwrite = TRUE)

# 这里是结尾部分也是不可以动的！
outputDS2 <- RxTextData(outputFile2, missingValueString = "", firstRowIsColNames = TRUE, quoteMark = "", fileSystem = hdfsFS2)
rxDataStep(inData = testDataSplitXdf2, outFile = outputDS2, overwrite = TRUE)
