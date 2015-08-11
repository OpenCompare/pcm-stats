 #!/usr/bin/env Rscript

setwd(".")
library(DBI)

con <- dbConnect(RSQLite::SQLite(), "../metrics/metrics.db")
# First make the request grouped by year to get changes
res <- dbSendQuery(con, "SELECT strftime('%Y', date) AS year,
    SUM(newFeatures) AS nf,
    SUM(delFeatures) AS df,
    SUM(newProducts) AS np,
    SUM(delProducts) AS dp
FROM metrics m
GROUP BY year")
metrics <- fetch(res, n=-1) # Get all results, not limited to 500

res <- dbSendQuery(con, "SELECT strftime('%Y', date) as year, count(id) as number FROM revisions GROUP BY year")
revisions <- fetch(res, n=-1)

# convert years as numeric
metrics$year <- as.numeric(metrics$year)
revisions$year <- as.numeric(revisions$year)

# Start PNG device driver to save output
png(filename="metrics_revisions.png", height=1200, width=1000, bg="white")

# Define colors to be used
plot_colors <- c("green", "red", "blue", "orange", "black")

# get the range for the x and y axis
daterange <- range(revisions$year, metrics$year)
ymax <- max(metrics$np, metrics$dp, metrics$nf, metrics$df, revisions$number)

# Define ticks and names
xticks = seq(daterange[1], daterange[2], 1)
lablist.x <- as.vector(xticks)
column_names <- c("New products", "Deleted products", "New features", "Deleted features", "Revisions")

# set up the empty plot
plot(NA, ann=F, ylim = c(0, ymax), xlim = daterange, xaxt = "n")

# Insert ticks with y in diagonal (to show all of the ticks)
axis(1, at=xticks, labels = lablist.x)

# add lines
lines(metrics$np ~ metrics$year, type="o", col=plot_colors[1], pch=17, lty="dotted", lwd = 2)
lines(metrics$dp ~ metrics$year, type="o", col=plot_colors[2], pch=18, lty="dotted", lwd = 2)
lines(metrics$nf ~ metrics$year, type="o", col=plot_colors[3], pch=19, lty="dotted", lwd = 2)
lines(metrics$df ~ metrics$year, type="o", col=plot_colors[4], pch=20, lty="dotted", lwd = 2)
lines(revisions$number ~ revisions$year, type="o", col=plot_colors[5], pch=16, lty="solid", lwd = 3)

# add a title and subtitle
title("Wikipedia matrices evolution", "Changes compared to revisions")

# Label the x and y axes
title(xlab= "Date (years)", ylab= "Quantity (units)")

# add a legend
legend("bottomright", column_names, cex=1, col=plot_colors, pch=16:20)

# Turn off device driver (to flush output to png)
dev.off()