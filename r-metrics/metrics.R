 #!/usr/bin/env Rscript

setwd(".")
library(DBI)

con <- dbConnect(RSQLite::SQLite(), "../metrics/metrics.db")
# Firts make the request grouped by id to get changes
res <- dbSendQuery(con, "SELECT strftime('%Y', date) AS year,
    SUM(newFeatures) AS nf,
    SUM(delFeatures) AS df,
    SUM(newProducts) AS np,
    SUM(delProducts) AS dp
FROM metrics m
GROUP BY year")
metrics <- fetch(res, n=-1)
metrics

# Nb matrices by date
res <- dbSendQuery(con, "SELECT year, SUM(nm) as nm FROM (
    SELECT strftime('%Y', m.date) AS year, MAX(m.nbMatrices) as nm
    FROM metrics m JOIN revisions r ON m.id = r.id
    GROUP BY r.title, year)
GROUP BY year")
matrices <- fetch(res, n=-1)
matrices

# convert column to specific type
metrics$year <- as.numeric(metrics$year)
metrics$nf <- metrics$nf
metrics$df <- metrics$df
metrics$np <- metrics$np
metrics$dp <- metrics$dp

# Start PNG device driver to save output to figure.png
png(filename="metrics.png", height=800, width=800, bg="white")

# Define colors to be used
plot_colors <- c("green", "red", "blue", "orange", "black")

# get the range for the x and y axis
daterange <- range(metrics$year)
ymax <- max(metrics$np, metrics$dp, metrics$nf, metrics$df, matrices$nm)
column_names <- c("New products", "Deleted products", "New features", "Deleted features", "Nb matrices")

# set up the plot
plot(NA, ann=F, ylim = c(0.9, ymax), xlim = daterange, log ="y")
axis(2, at=seq(0, ymax, 50))

# add lines
lines(metrics$np ~ metrics$year, type="l", col=plot_colors[1], pch=21)
lines(metrics$dp ~ metrics$year, type="l", col=plot_colors[2], pch=22)
lines(metrics$nf ~ metrics$year, type="l", col=plot_colors[3], pch=23)
lines(metrics$df ~ metrics$year, type="l", col=plot_colors[4], pch=24)
lines(matrices$nm ~ matrices$year, type="o", col=plot_colors[5], pch=25)

# add a title and subtitle
title("Wikipedia matrices evolution")

# Label the x and y axes with dark green text
title(xlab= "Date (years)", ylab= "Quantity (units)")

# add a legend
legend("topleft", column_names, cex=0.8, col=plot_colors, pch=21:25)

# Turn off device driver (to flush output to png)
dev.off()