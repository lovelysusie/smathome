# accept the arguments from command line
args = (commandArgs(TRUE))

if (length(args) < 2) {
    stop ("At least 2 argument must be supplied. 1st for the location of the 
        dataset. 2nd is the directory on hdfs for the output model storage", call.=FALSE) 
}

# print the arguments for debugging purposes
# Set the spark context and define the executor memory and processor cores. 



  outputFile2 <- args[2] #这里只是一个路径

  inputFile2  <- args[1] #这里也只是一个路径
  #inputFile1  <- args[1]
  #name of the final model object
  #tempLocalModelFile <- 'testadfmodel'
  filePrefix <- "rspark/data"
  rxSetComputeContext(RxSpark(hdfsShareDir = "rspark/data", consoleOutput=TRUE, executorMem="2g", executorCores = 2, driverMem="1g", executorOverheadMem="1g") )
  hdfsFS2 <- RxHdfsFileSystem()

# 这里需要只是导入当天的数据和前一天的数据
# 这个是今天的
today <- Sys.Date()
todaydate <- format(today, format="%Y-%m-%d")
strDates <- as.character(todaydate)
print(strDates)


inputFile2 <- paste(inputFile2, strDates, sep="/")
print(inputFile2)

# 这个是昨天的
#yesterday <- Sys.Date()-1
#yesterdaydate <-format(yesterday,format="%Y-%m-%d")
#yesterdaystr <-as.character(yesterdaydate)
#print(yesterdaystr)

#inputFile1 <- paste(inputFile1, yesterdaystr, sep="/")
#print(inputFile1)


#inputFile2是一个路径

#然后开始处理 结果是路径  
xdfOutFile2 <- file.path(filePrefix, "testadfxdf2")
#xdfOutFile1 <- file.path(filePrefix, "testadfxdf1")

#set the path of output
testDataSplitXdfFile2 <- file.path(filePrefix, "testadfSplitXdf2")
#testDataSplitXdfFile1 <- file.path(filePrefix, "testadfSplitXdf1")
#testDataSplitXdfFile <- file.path(filePrefix, "testadfSplitXdf")

testDataClasses2 <- c(name = "character",starttime = "character",
                        hubid = "character",tasklocation = "character", 
                        previoustime = "character",
                        timegap = "numeric" )
#testDataClasses1 <- c(name = "character",starttime = "character",
#                        hubid = "character",tasklocation = "character", 
#                        previoustime = "character",
#                        timegap = "numeric" )

#loading data from blob, testDataDS2 is Formal Class RxtextData
testDataDS2 <- RxTextData(file = inputFile2, fileSystem = hdfsFS2, delimiter = ",", firstRowIsColNames = TRUE,
                     colClasses = testDataClasses2)
#testDataDS1 <- RxTextData(file = inputFile1, fileSystem = hdfsFS2, delimiter = ",", firstRowIsColNames = TRUE,
#                     colClasses = testDataClasses2)

#xdfOut2/1 is Formal Class RxxdfData
xdfOut2 <- RxXdfData(file = xdfOutFile2, fileSystem = hdfsFS2)
#xdfOut1 <- RxXdfData(file = xdfOutFile1, fileSystem = hdfsFS2)


#testDataDSXdf2/1 is Formal Class RxxdfData, outFiles是
testDataDSXdf2 <- rxImport(inData = testDataDS2, outFile = xdfOut2,
                      createCompositeSet = TRUE,
                      overwrite = TRUE)

#testDataDSXdf1 <- rxImport(inData = testDataDS1, outFile = xdfOut1,
#                      createCompositeSet = TRUE,
#                      overwrite = TRUE)

#rxGetInfo( data = testDataDSXdf2, getVarInfo = TRUE)
#rxGetInfo( data = testDataDSXdf1, getVarInfo = TRUE)

#把输出设定成一个空的xdf文件
testDataSplitXdf2 <- RxXdfData(file = testDataSplitXdfFile2, fileSystem = hdfsFS2);
#testDataSplitXdf1 <- RxXdfData(file = testDataSplitXdfFile1, fileSystem = hdfsFS2);
#testDataSplitXdf <- RxXdfData(file = testDataSplitXdfFile, fileSystem = hdfsFS2);

# drop some variables
varsToDrop = c("name")

# outFile的设定在这里在这里

rxDataStep(inData = testDataDSXdf2, outFile = testDataSplitXdf2,
           varsToDrop = varsToDrop,
           rowSelection = (tasklocation == 'Bedroom' & hubid==unique(hubid)[2]),
           overwrite = TRUE)

#rxDataStep(inData = testDataDSXdf1, outFile = testDataSplitXdf1,
#           varsToDrop = varsToDrop,
#           rowSelection = (tasklocation == 'Bedroom' & hubid==unique(hubid)[2]),
#           overwrite = TRUE)

mydataframe2 <-rxDataStep(testDataSplitXdf2)
mydataframe2 <-data.frame(mydataframe2)
print(length(mydataframe2))
#mydataframe1 <-rxDataStep(testDataSplitXdf1)
#mydataframe1 <-data.frame(mydataframe1)
#print(length(mydataframe1))
#mydataframe <-rbind(mydataframe1,mydataframe2)
#print(length(mydataframe))
#said here is problem testadfxdf2

rxDataStep( inData = mydataframe2, outFile = testDataSplitXdf2, overwrite = TRUE)

#rxDataStep(testDataSplitXdf,numRows = 5)
Sys.Date()
Sys.time()

outputDS2 <- RxTextData(outputFile2, missingValueString = "", firstRowIsColNames = TRUE, quoteMark = "", fileSystem = hdfsFS2)
# outputDS2 is a Formal Class RxxdfData
rxDataStep(inData = testDataSplitXdf2, outFile = outputDS2, overwrite = TRUE)




