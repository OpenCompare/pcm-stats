# pcm-stats

## Process

### Grab wikitext from page revisions

Due to performance issues, it's preferable to add these options to the JVM (see http://www.oracle.com/technetwork/java/javase/tech/vmoptions-jsp-140102.html for further explaination)
 - -Xmx4096m
 - -XX:-UseParallelOldGC
 - -XX:InitiatingHeapOccupancyPercent=0

### Parse the wikitext to make metrics on matrix evolution

Use of org.opencompare.{api, io} dependencies. Especially `MediaWikipediaApi` class.

### Compute metrics to obtain graphical interpretation

--In development--
