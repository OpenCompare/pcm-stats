 #!/usr/bin/env Rscript

setwd(".")
library(DBI)

con <- dbConnect(RSQLite::SQLite(), "../metrics/metrics.db")
# Firts make the request grouped by id
res <- dbSendQuery(con, "SELECT strftime('%Y', date) AS year,
    SUM(newFeatures) AS nf,
    SUM(delFeatures) AS df,
    SUM(newProducts) AS np,
    SUM(delProducts) AS dp,
    (SELECT SUM(nbMatrices) / COUNT(id) FROM metrics WHERE id = m.id GROUP BY id) AS nm
FROM metrics m
GROUP BY id, year")
metrics <- fetch(res, n=-1)

# convert column to specific type
metrics$year <- as.numeric(metrics$year)
metrics$nf <- metrics$nf / 10000
metrics$df <- metrics$df / 10000
metrics$np <- metrics$np / 10000
metrics$dp <- metrics$dp / 10000

# Start PNG device driver to save output to figure.png
png(filename="metrics.png", height=800, width=800, bg="white")

# Define colors to be used
plot_colors <- c("green", "red", "blue", "orange", "black")

# get the range for the x and y axis
daterange <- range(metrics$year)
ymax <- max(metrics$np, metrics$dp, metrics$nf, metrics$df, metrics$nm)
column_names <- c("New products (x10000)", "Deleted products (x10000)", "New features (x10000)", "Deleted features (x10000)", "Nb matrices (unit)")

# set up the plot
plot(NA, ann=F, ylim = c(0, ymax), xlim = daterange)

# add lines
lines(metrics$np ~ metrics$year, type="l", col=plot_colors[1], pch=21)
lines(metrics$dp ~ metrics$year, type="l", col=plot_colors[2], pch=22)
lines(metrics$nf ~ metrics$year, type="l", col=plot_colors[3], pch=23)
lines(metrics$df ~ metrics$year, type="l", col=plot_colors[4], pch=24)
lines(metrics$nm ~ metrics$year, type="o", col=plot_colors[5], pch=25)

# add a title and subtitle
title("Wikipedia matrices evolution")

# Label the x and y axes with dark green text
title(xlab= "Date (years)", ylab= "Quantity (relative units)")

# add a legend
legend("topleft", column_names, cex=0.8, col=plot_colors, pch=21:25)

# Turn off device driver (to flush output to png)
dev.off()