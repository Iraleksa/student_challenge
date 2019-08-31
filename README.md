# student_challenge
Given a dataframe with two columns, "timestamp" and "x", write a function that returns 
x aggregated by a chosen measure (mean, mode...) and a chosen time unit (day, month...)

Test function is tested with this data (R code)
set.seed(888)
timestamp = seq(from = ISOdate(2018,11,15), to = ISOdate(2019,1,15), by = "min")
x = rnorm(length(timestamp))
mydf <- data.frame(timestamp, x)

write.csv(mydf,file = "mydf.csv", row.names = FALSE)
