library(lubridate)
library(timevis)
wakeup <-read.csv(file.choose())
wakeup$date <-as.Date(wakeup$date)
wakeup$wakeup <-strptime(wakeup$wakeup, "%Y-%m-%d %H:%M")
wakeup$sleep <-strptime(wakeup$sleep, "%Y-%m-%d %H:%M")


i=2
j=nrow(wakeup)+1
while (i<j) {
  wakeup$wakeup[i]=wakeup$wakeup[i]-(i-1)*86400
  i=i+1
}
wakeup$wakeup=wakeup$wakeup+86400

i=2
j=nrow(wakeup)+1
while (i<j) {
  wakeup$sleep[i]=wakeup$sleep[i]-(i-1)*86400
  i=i+1
}
############################
wakeup$wakeup <-wakeup$wakeup-86400*2
###########################
names(wakeup) <-c("date","sleep","wakeup")
#######
data <- data.frame(
  id      = 1:nrow(wakeup),
  content = wakeup$date,
  start   = wakeup$sleep,
  end     = wakeup$wakeup
)
#######
##????????????
timevis(data)
######
