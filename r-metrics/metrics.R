 #!/usr/bin/env Rscript

setwd("~/IdeaProjects/OpenCompare/pcm-stats/r-metrics")
#setwd(".")
library(DBI)

con <- dbConnect(RSQLite::SQLite(), "../metrics/metrics.db")
dbListTables(con)
res <- dbSendQuery(con, "SELECT date, newFeatures, delFeatures, newProducts, delProducts, nbMatrices FROM metrics")
metrics <- dbFetch(res)

# Start PNG device driver to save output to figure.png
png(filename="metrics.png", height=800, width=800, bg="white")

# convert column to specific type
metrics$date <- format(as.POSIXlt(metrics$date), "%Y")
nbMatrices <- rowsum(metrics$nbMatrices, metrics$date)
nbMatrices
newFeatures <- rowsum(metrics$newFeatures, metrics$date)
newFeatures
delFeatures <- rowsum(metrics$delFeatures, metrics$date)
delFeatures
newProducts <- rowsum(metrics$newProducts, metrics$date)
newProducts
delProducts <- rowsum(metrics$delProducts, metrics$date)
delProducts

# Define colors to be used
plot_colors <- c("green", "red", "blue", "orange", "black")

# get the range for the x and y axis
daterange <- range(metrics$date)
daterange
yrange <- range(newProducts[,1], delProducts[,1], newFeatures[,1], delFeatures[,1], nbMatrices[,1])
yrange
column_names <- c("New products", "Deleted products", "New features", "Deleted features", "Nb matrices")

# set up the plot
plot(newProducts, ann=F, ylim = yrange, type="l", col=plot_colors[1], pch=21)

# add lines
lines(delProducts, type="l", col=plot_colors[2], pch=22)
lines(newFeatures, type="l", col=plot_colors[3], pch=23)
lines(delFeatures, type="l", col=plot_colors[4], pch=24)
lines(nbMatrices, type="o", col=plot_colors[5], pch=25)

# add a title and subtitle
title("Wikipedia matrices evolution", "Products and Features in matrices")

# Label the x and y axes with dark green text
title(xlab= "Date (years)", ylab= "Nb (unit)")

# add a legend
legend("topleft", column_names, cex=0.8, col=plot_colors, pch=21:25)

# Turn off device driver (to flush output to png)
dev.off()