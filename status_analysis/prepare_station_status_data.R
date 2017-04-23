library(padr)
library(ggplot2)
library(dplyr)
library(forecast)


get_data <- function() {
  # Gets the station data
  #
  # Returns:
  #   The station data as a dataframe
  
  df <- read.csv("station_197_extract.csv")
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
  #   Dataframe with timestamp and min bikes available
  
  df1 <- df %>% 
    thicken('hour', by=timestamp()) %>% 
    group_by(timestamp_hour) %>%
    summarise(hour_amount = min(bikes_available)) %>% 
    pad() %>% 
    fill_by_value(hour_amount)
  names(df1) <- c("time", "bikes")
  return(df1)
  
#  ggplot(df_bikes, aes(time, bikes)) + geom_point() +  geom_smooth() +
#    labs(x="Time", y="Bikes Available", title="Bike Availability July/August")
}




df <- get_data()
df_bikes <- reduce_to_hours(df)
tser <- ts(df_bikes$bikes, frequency = 24, start = c(2015, 9))
df_forecast <- HoltWinters(tser)
plot(df_forecast)
pred <- predict(df_forecast, n.ahead = 24, level = .85)

