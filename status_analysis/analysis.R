library(padr)
library(ggplot2)
library(dplyr)
library(forecast)
library(reshape2)


get_data <- function() {
  # Gets the station data
  #
  # Returns:
  #   The station data as a dataframe
  
  df <- read.csv("../data/station_197_extract.csv")
  df$timestamp<-as.POSIXct(df$timestamp,format="%Y-%m-%d %H:%M")
  return(df)
}

reduce_to_hours <- function(df) {
  # Aggregates by hour and fills in missing values with the previous.
  #
  # Args:
  #   df: dataframe containing station data
  #
  # Returns:
  #   Dataframe with timestamp, min bikes available, and min docks available
  
  dfb <- df %>% 
    thicken('hour', by=timestamp()) %>% 
    group_by(timestamp_hour) %>%
    summarise(hour_amount = min(bikes_available)) %>% 
    pad() %>% 
    fill_by_value(hour_amount)
  names(dfb) <- c("time", "bikes")
  
  dfd <- df %>% 
    thicken('hour', by=timestamp()) %>% 
    group_by(timestamp_hour) %>%
    summarise(hour_amount = min(docks_available)) %>% 
    pad() %>% 
    fill_by_value(hour_amount)
  names(dfd) <- c("time", "docks")
  
  dfboth <- merge(dfb, dfd, by="time")
  return(dfboth)
}




df <- get_data()
df <- reduce_to_hours(df)

# Bike prediction
tsb <- ts(df$bikes, frequency = 24, start = c(2015, 9))
hwb <- HoltWinters(tsb)
predb <- predict(hwb, n.ahead = 24, level = .85)
# plot(df_forecast)

# Dock prediction
tsd <- ts(df$docks, frequency = 24, start = c(2015, 9))
hwd <- HoltWinters(tsd)
predd <- predict(hwd, n.ahead = 24, level = .85)

# Join predictions and plot
predicted <- data.frame(predb, predd)
names(predicted) <- c("bikes", "docks")
predicted$hour <- as.numeric(rownames(predicted))
predictedm <- melt(predicted, id.vars = "hour")

ggplot(data = predictedm, aes(x = hour, y = value, group = variable, colour = variable)) + 
  geom_line() + 
  ggtitle("Bike/dock availability 9/1/2016") + 
  xlab("Hour") + ylab("Bikes/Docks")
