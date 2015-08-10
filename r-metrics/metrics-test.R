 #!/usr/bin/env Rscript

setwd(".")
library(DBI)

con <- dbConnect(RSQLite::SQLite(), "../metrics/metrics.db")
res <- dbSendQuery(con, "
SELECT strftime('%Y', date) as year,
    sum(newFeatures) as nf,
    sum(delFeatures) as df,
    sum(newProducts) as np,
    sum(delProducts) as dp,
    sum(nbMatrices) as nm
FROM metrics
GROUP BY year")
metrics <- dbFetch(res)
metrics

# Start PNG device driver to save output to figure.png
png(filename="metrics-test.png", height=800, width=800, bg="white")

# Define colors to be used
plot_colors <- c("green", "red", "blue", "orange", "black")

# Convert some types
metrics$year <- as.numeric(metrics$year)

# get the range for the x and y axis
daterange <- range(metrics$year)
daterange
yrange <- range(metrics$np, metrics$dp, metrics$nf, metrics$df, metrics$nm)
yrange
column_names <- c("New products", "Deleted products", "New features", "Deleted features", "Nb matrices")

# set up the plot
plot(NA, ann=F, ylim = yrange, xlim = daterange)

# add lines
lines( metrics$np, type="l", col=plot_colors[1], pch=21)
lines(metrics$year ~ metrics$dp, type="l", col=plot_colors[2], pch=22)
lines(metrics$year ~ metrics$nf, type="l", col=plot_colors[3], pch=23)
lines(metrics$year ~ metrics$df, type="l", col=plot_colors[4], pch=24)
lines(metrics$year ~ metrics$nm, type="o", col=plot_colors[5], pch=25)

# add a title and subtitle
title("Wikipedia matrices evolution", "Products and Features in matrices")

# Label the x and y axes with dark green text
title(xlab= "Date (years)", ylab= "Nb (unit)")

# add a legend
legend("topleft", column_names, cex=0.8, col=plot_colors, pch=21:25)

# Turn off device driver (to flush output to png)
dev.off()