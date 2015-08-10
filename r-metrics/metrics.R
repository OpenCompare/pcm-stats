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

# Secondly, get all matrices number by year
res <- dbSendQuery(con, "SELECT year, SUM(nm) as nm FROM (
    SELECT strftime('%Y', m.date) AS year, MAX(m.nbMatrices) as nm
    FROM metrics m JOIN revisions r ON m.id = r.id
    GROUP BY r.title, year)
GROUP BY year")
matrices <- fetch(res, n=-1)

# convert years as numeric
metrics$year <- as.numeric(metrics$year)

# Start PNG device driver to save output
png(filename="metrics.png", height=1200, width=1000, bg="white")

# Define colors to be used
plot_colors <- c("green", "red", "blue", "orange", "black")

# get the range for the x and y axis
daterange <- range(metrics$year)
ymax <- max(metrics$np, metrics$dp, metrics$nf, metrics$df, matrices$nm)

# Define ticks and names
xticks = seq(daterange[1], daterange[2], 1)
yticks = c(1, 2, 5, 10, 30, 100, 300, 1000, 3000, 10000, 30000, 50000, 100000, 150000, 200000, 250000, 300000)
lablist.x <- as.vector(xticks)
lablist.y <- as.vector(yticks)
column_names <- c("New products", "Deleted products", "New features", "Deleted features", "Nb matrices")

# set up the empty plot
plot(NA, ann=F, ylim = c(0.9, ymax), xlim = daterange, log ="y",
 panel.first=abline(v = xticks, h = yticks, lty=1, col="gray"),
  xaxt = "n", yaxt="n")

# Insert ticks with y in diagonal (to show all of the ticks)
axis(1, at=xticks, labels = lablist.x)
axis(2, at=yticks, labels = FALSE)
text(y = yticks, par("usr")[1], labels = lablist.y, srt = 45, pos = 2, xpd = TRUE)

# add lines
lines(metrics$np ~ metrics$year, type="o", col=plot_colors[1], pch=21)
lines(metrics$dp ~ metrics$year, type="o", col=plot_colors[2], pch=22)
lines(metrics$nf ~ metrics$year, type="o", col=plot_colors[3], pch=23)
lines(metrics$df ~ metrics$year, type="o", col=plot_colors[4], pch=24)
lines(matrices$nm ~ matrices$year, type="o", col=plot_colors[5], pch=25)

# add a title and subtitle
title("Wikipedia matrices evolution")

# Label the x and y axes
title(xlab= "Date (years)", ylab= "Quantity (units)")

# add a legend
legend("bottomright", column_names, cex=0.8, col=plot_colors, pch=21:25)

# Turn off device driver (to flush output to png)
dev.off()