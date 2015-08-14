 #!/usr/bin/env Rscript

setwd(".")
library(DBI)

con <- dbConnect(RSQLite::SQLite(), "../metrics/metrics.db")
# Get all users by year
res <- dbSendQuery(con, "SELECT strftime('%Y', date) as year, count(unique(author)) as author FROM revisions GROUP BY year")
users <- dbFetch(res)

# Get all revisions by year
res <- dbSendQuery(con, "SELECT strftime('%Y', date) as year, count(id) as number FROM revisions GROUP BY year")
revisions <- dbFetch(res)

# Secondly, get all matrices number by year
res <- dbSendQuery(con, "SELECT year, SUM(nm) as nm FROM (
    SELECT strftime('%Y', m.date) AS year, MAX(m.nbMatrices) as nm
    FROM metrics m JOIN revisions r ON m.id = r.id
    GROUP BY r.title, year)
GROUP BY year")
matrices <- fetch(res, n=-1)

# Start PNG device driver to save output to figure.png
png(filename="revisions_matrices.png", height=1200, width=1000, bg="white")

# Define ticks and names
column_names <- c("Matrices", "Revisions", "Authors")

# convert years as numeric
revisions$year <- as.numeric(revisions$year)
matrices$year <- as.numeric(matrices$year)
users$year <- as.numeric(users$year)

# get the range for the x and y axis
daterange <- range(revisions$year, matrices$year)
yrange <- range(revisions$number, matrices$nm, users$author)

# set up the plot
plot(NA, ann=F, xlim = daterange, ylim = yrange)
lines(revisions$number ~ revisions$year, type="l", lty = "solid", col="orange", pch=15, lwd = 2)
lines(matrices$nm ~ matrices$year, type="l", lty = "longdash", col="blue", pch=16, lwd = 2)
lines(users$author ~ users$year, type="l", lty = "dash", col="darkgreem", pch=17, lwd = 2)

# add a title and subtitle
title("Wikipedia matrix evolution", "Matrices compared to revisions")

# Label the x and y axes with dark green text
title(xlab= "Date (years)", ylab= "Quantity (unit)")

# add a legend
legend("topleft", column_names, cex=1, col=c("orange", "blue", "darkgreen"), pch=15:17)

# Turn off device driver (to flush output to png)
dev.off()