# pcm-stats (Wikipedia metrics generation on page comparison tables)

## Goal

This project aims to retreive the maximum information about matrix (comparison tables) creation and edition history on the Wikipedia page revisions to give visibility and drive the functional developpement of the main project, OpenCompare.

## Limits

As wikitext is an ambiguous and human editing language, there are some limitations towards matrices identification inside a page. Like moved matrices inside different section (used to name them) unables the facility to identify and compare same matrices of different revisions. The position inside the page can also vary, this main problem remains the same.

An other difficulty is to identify pages containing matrices, there is no such tools on Wikipedia API to do so. And parsing all pages revisions to search matrices would be a huge server resources consuption (especialy in an international context).

## Database

Sqlite has been firstly used to prevent issues and data loss from originaly csv file accessed throught threads. Secondly, it allows to create custom select requests to manage metrics processing easier than in a csv file.

### Metamodel

![iDatabase metamodel](db-diagram.png)

## Functional processes

### Main launcher

The main class is `org.opencompare.stats.Launcher` which launches sequentialy the two main processes.

### Grab wikitext from page revisions

This task is done by `org.opencompare.stats.processes.Revisions` class. It reads a custom csv page list to know wich page to parse.
This process is splitted into several other classes such as :
  

#### Performance

##### Issues

Due to performance issues (tested in a quadcore AMD processor with 8GB RAM, it lasts 7 hours), it's preferable to add these options to the JVM (see http://www.oracle.com/technetwork/java/javase/tech/vmoptions-jsp-140102.html for further explaination) to ensure any Java stack and GC limitation exceptions :
 - -Xmx5120m
 - -XX:-UseParallelOldGC
 - -XX:InitiatingHeapOccupancyPercent=10

##### Resources

To spare some Wikipedia servers resources (and already time consuption) when a revision has been processed, the revision's associated wikitext is not retreived next times the script is launched.
If the output folder `metrics` is deleted, the script regenerates all the necessary files before processing.

### Parse the wikitext to make metrics on matrix evolution

This task is done by `org.opencompare.stats.processes.Metrics` class.

Use of org.opencompare.{api, io} dependencies. Especially `MediaWikipediaApi` class to retreive revisions data and metadatas.

#### Performance issues

Like the grabbing process, a line previously created will not be reprocessed.

### Compute metrics to obtain graphical interpretation

Use of R to process the graphical representation of metrics. The script is located in `r-metrics`.

![Example of metrics graph output](metrics.png)

## R

Once main processes finished, launch the R scripts to process graphical representation.

    cd r-metrics
    Rscript metrics_log.R => top have a full logarithmic metrics view
    Rscript metrics_revisions.R => to have a linear comparison between changes and revisions
    Rscript revisions_matrices.R => to have a linear comarison between revisions and matrices
    
### Depencencies

    RSqlite
