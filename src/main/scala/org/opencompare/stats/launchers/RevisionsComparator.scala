package org.opencompare.stats.launchers

import org.apache.log4j.{FileAppender, Level, Logger}
import org.joda.time.DateTime
import org.opencompare.api.java.util.{ComplexePCMElementComparator, DiffResult}
import org.opencompare.api.java.{PCM, PCMContainer}
import org.opencompare.io.wikipedia.io.{MediaWikiAPI, WikiTextLoader}
import org.opencompare.stats.utils.{DataBase, WikiTextKeepTemplateProcessor}

import scala.collection.JavaConversions._
import scala.io.Source

/**
 * Created by smangin on 30/07/15.
 */
class RevisionsComparator(db : DataBase, api : MediaWikiAPI, wikitextPath : String, appender : FileAppender, level : Level) extends Thread {

  private val logger = Logger.getLogger("metrics.comparator")
  logger.addAppender(appender)
  logger.setLevel(level)

  private val wikiLoader = new WikiTextLoader(new WikiTextKeepTemplateProcessor(api))

  def process(title : String, content : List[Map[String, Any]]): Map[String, Int] = {
    var newestId : Int = 0
    var oldestId : Int = 0
    var oldestPcm : PCM = null
    var newestPcm : PCM = null
    var oldestContainers : List[PCMContainer] = null
    var oldestContainer : Option[PCMContainer] = null
    var newestContainers : List[PCMContainer] = null
    var revisionsSize = content.size
    var revisionsDone = 0
    var wikitext = ""
    var diff : DiffResult = null

    // Sort by revision Id newer to older
    content.sortBy(line => line.apply("id").asInstanceOf[Int]).reverse.foreach(line => {
      val lang = line.get("lang").get.toString
      val date = line.get("date").get.toString
      oldestId = line.get("id").get.asInstanceOf[Int]
      try {
        // Get the wikitext code
        val wikifile = Source.fromFile(wikitextPath + title + "/" + oldestId + ".wikitext")
        wikitext = wikifile.mkString
        wikifile.close()
        // Parse it through wikipedia miner
        oldestContainers = wikiLoader.mine(lang, wikitext, title).toList
        //  It should avoid multiple unnamed matrix
        if (oldestContainers.count(container => container.getPcm.getName == (title + " -   ")) <= 1) {

          // If first line, go next
          if (newestContainers != null) {

            // Get containers size
            val oldestContainersSize = oldestContainers.size
            val newestContainersSize = newestContainers.size

            // For each matrix in the page
            for (previousContainer <- newestContainers) {
              newestPcm = previousContainer.getPcm

              // Search for a matrix in the current page only if more than 1 matrix inside
              // FIXME : if the matrix is a new one, the results won't be objective
              // cf: Comparison_of_Dewey_and_Library_of_Congress_subject_classification for matrix name change only through title
              if (oldestContainersSize == 1) {
                oldestContainer = Option[PCMContainer](oldestContainers.get(0))
              } else {
                // FIXME : A bug is present while parsing multiple matrices inside the same section
                oldestContainer = oldestContainers.find(container => container.getPcm.getName == newestPcm.getName)
              }

              if (oldestContainer.isDefined) {
                // If the matrix exists in the current line
                oldestPcm = oldestContainer.get.getPcm
                // Treatment by comparing previous line with current one to populate previous container line
                diff = oldestPcm.diff(newestPcm, new ComplexePCMElementComparator)
                db.syncExecute("insert into metrics values(" +
                  oldestId+", "+
                  "'"+oldestPcm.getName.replace("'", "")+"', "+
                  "'" + DateTime.parse(date) + "', "+
                  newestId+", "+
                  oldestContainersSize+", "+
                  (oldestContainersSize - newestContainersSize)+", "+
                  diff.getFeaturesOnlyInPCM2.size()+", "+
                  diff.getFeaturesOnlyInPCM1.size()+", "+
                  diff.getProductsOnlyInPCM2.size()+", "+
                  diff.getProductsOnlyInPCM1.size()+", "+
                  diff.getDifferingCells.size()+")")
              } else {
                if (oldestContainers.size == 0) {
                  logger.warn("[" + oldestId + "] >> [" + newestId + "] first matrix '" + newestPcm.getName + "'")
                  // New matrix
                } else {
                  logger.warn("[" + oldestId + "] >> [" + newestId + "] deleted matrix '" + newestPcm.getName + "'")
                  // Renamed or deleted matrix
                }
                // Otherwize populate metrics with the new matrix properties
                // FIXME : find a better way to show the difference
                //db.syncExecute("insert into metrics values(" +
                //  previousId+", "+
                //  "'"+previousPcm.getName.replace("'", "")+"', "+
                //  currentId+", "+
                //  previousContainersSize+", "+
                //  currentContainersSize+", "+
                //  1+", "+
                //  0+", "+
                //  0+", "+
                //  0+", "+
                //  0+", "+
                //  0+")")
              }
            }
          }
          revisionsDone += 1
        }
        // The current container becomes the new matrix to diff
        newestContainers = oldestContainers
      } catch {
        case e: Exception => {
          logger.error("[" + oldestId + "] matrix '" + title + "' =>" + e.getLocalizedMessage)
          logger.error(e.getStackTraceString)
        }
      }
      newestId = oldestId
    })
    Map[String, Int](("revisionsDone", revisionsDone), ("revisionsSize", revisionsSize))
  }

}
