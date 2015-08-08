package org.opencompare.stats.utils

import org.apache.log4j.{FileAppender, Level, Logger}
import org.joda.time.DateTime
import org.opencompare.api.java.util.ComplexePCMElementComparator
import org.opencompare.api.java.{PCM, PCMContainer}
import org.opencompare.io.wikipedia.io.{MediaWikiAPI, WikiTextLoader}

import scala.collection.JavaConversions._
import scala.io.Source

/**
 * Created by smangin on 30/07/15.
 *
 * Browse sequentially a list of revision sorted by id (newest to oldest).
 * Compare each revisions with the closiest oldest one.
 *
 */
class RevisionsComparator(db : DatabaseSqlite, api : MediaWikiAPI, wikitextPath : String, appender : FileAppender, level : Level) extends Thread {

  private val logger = Logger.getLogger("metrics.comparator")
  logger.addAppender(appender)
  logger.setLevel(level)
  var newestId : Int = 0
  var oldestId : Int = 0
  var oldestPcm : PCM = null
  var newestPcm : PCM = null
  var oldestContainers : List[PCMContainer] = null
  var oldestContainer : Option[PCMContainer] = null
  var newestContainers : List[PCMContainer] = null
  var revisionsDone = 0
  var wikitext = ""

  private val wikiLoader = new WikiTextLoader(new WikiTextKeepTemplateProcessor(api))

  def compare(title : String, content : List[Map[String, Any]]): Map[String, Int] = {
    var revisionsSize = content.size
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
        // The current (oldiest) container becomes the new container to process
        newestContainers = process(oldestContainers, newestContainers, title, date)
      } catch {
        case e: Exception => {
          logger.error(title + " -- " + oldestId + " -- " + e.getLocalizedMessage)
          logger.error(e.getStackTraceString)
        }
      }
      newestId = oldestId
    })
    Map[String, Int](("revisionsDone", revisionsDone), ("revisionsSize", revisionsSize))
  }

  private def process(oldestContainers : List[PCMContainer], newestContainers : List[PCMContainer], title : String, date : String): List[PCMContainer] = {

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
          save(date, oldestContainersSize, newestContainersSize)
        } else {
          if (oldestContainers.size == 0) {
            logger.warn(title + " -- " + oldestId + " -- " + "first matrix '" + newestPcm.getName + "'")
            // New matrix
          } else {
            logger.warn(title + " -- " + oldestId + " -- " + " deleted matrix '" + newestPcm.getName + "'")
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
    oldestContainers
  }

  private def save(date : String, oldestContainersSize : Int, newestContainersSize : Int): Unit = {
    // Treatment by comparing previous line with current one to populate previous container line
    val diff = newestPcm.diff(oldestPcm, new ComplexePCMElementComparator)
    // Do not save empty values
    if (diff.getFeaturesOnlyInPCM1.size() != 0 || diff.getFeaturesOnlyInPCM2.size() != 0
      || diff.getProductsOnlyInPCM1.size() != 0 || diff.getProductsOnlyInPCM2.size() != 0
      || (newestContainersSize - oldestContainersSize) != 0 || diff.getDifferingCells.size() != 0) {
      db.execute("insert into metrics values(" +
        newestId + ", " +
        "'" + newestPcm.getName.replace("'", "") + "', " +
        "'" + DateTime.parse(date) + "', " +
        oldestId + ", " +
        newestContainersSize + ", " +
        (newestContainersSize - oldestContainersSize) + ", " +
        diff.getFeaturesOnlyInPCM1.size() + ", " +
        diff.getFeaturesOnlyInPCM2.size() + ", " +
        diff.getProductsOnlyInPCM1.size() + ", " +
        diff.getProductsOnlyInPCM2.size() + ", " +
        diff.getDifferingCells.size() + ")")
    }
  }
}
