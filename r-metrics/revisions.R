 #!/usr/bin/env Rscript

setwd("~/IdeaProjects/OpenCompare/pcm-stats/r-metrics")
#setwd(".")
library(DBI)

con <- dbConnect(RSQLite::SQLite(), "../metrics/metrics.db")
dbListTables(con)
res <- dbSendQuery(con, "SELECT date, count(id) as number FROM revisions GROUP BY date")
metrics <- dbFetch(res)

# Start PNG device driver to save output to figure.png
png(filename="revisions.png", height=800, width=800, bg="white")

# convert column to specific type
metrics$date <- format(as.POSIXlt(metrics$date), "%Y-%m")
metrics <- rowsum(metrics$number, metrics$date)
metrics

# get the range for the x and y axis
yrange <- range(metrics[,1])
yrange

# set up the plot
plot(metrics, ann=F, ylim = yrange, type="l")

# add a title and subtitle
title("Wikipedia matrix revisions evolution")

# Label the x and y axes with dark green text
title(xlab= "Date (years)", ylab= "Nb (unit)")

# Turn off device driver (to flush output to png)
dev.off()