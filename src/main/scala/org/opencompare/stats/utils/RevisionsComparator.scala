package org.opencompare.stats.utils

import org.apache.log4j.{FileAppender, Level, Logger}
import org.joda.time.DateTime
import org.opencompare.api.java.util.ComplexePCMElementComparator
import org.opencompare.api.java.{PCM, PCMContainer}
import org.opencompare.io.wikipedia.io.{MediaWikiAPI, WikiTextLoader}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 * Created by smangin on 30/07/15.
 *
 * Browse sequentially a list of revision sorted by id (newest to oldest).
 * Compare each revisions with the closiest oldest one.
 *
 */
class RevisionsComparator(api : MediaWikiAPI, wikitextPath : String, appender : FileAppender, level : Level) extends Thread {

  private val logger = Logger.getLogger("metrics.comparator")
  logger.addAppender(appender)
  logger.setLevel(level)
  var newestId : Int = 0
  var oldestId : Int = 0
  var oldestContainers : List[PCMContainer] = null
  var newestContainers : List[PCMContainer] = null
  var revisionsDone = 0
  var metrics : ListBuffer[Map[String, Any]] = ListBuffer.empty

  private val wikiLoader = new WikiTextLoader(new WikiTextKeepTemplateProcessor(api))

  def getMetrics(): List[Map[String, Any]] = {
    metrics.toList
  }

  def compare(title : String, content : List[Map[String, Any]]): Map[String, Int] = {
    metrics = ListBuffer.empty
    var revisionsSize = content.size
    // Sort by revision Id newer to older
    content.sortBy(line => line.apply("id").asInstanceOf[Int]).reverse.foreach(line => {
      val lang = line.get("lang").get.toString
      val date = line.get("date").get.toString
      oldestId = line.get("id").get.asInstanceOf[Int]
      try {
        // Get the wikitext code
        val wikifile = Source.fromFile(wikitextPath + title + "/" + oldestId + ".wikitext")
        val wikitext = wikifile.mkString
        wikifile.close()
        // Parse it through wikipedia miner
        oldestContainers = wikiLoader.mine(lang, wikitext, title).toList

        var oldestPcm : PCM = null
        var newestPcm : PCM = null
        var oldestContainer : Option[PCMContainer] = null

        // Get containers size
        val oldestContainersSize = oldestContainers.size
        // Get container unnamed matrix
        val tooUnamedMatrices = oldestContainers.count(container => container.getPcm.getName == (title + " -   ")) > 1

        //  It should avoid multiple unnamed matrix
        if (!tooUnamedMatrices) {

          // If not first line
          if (newestContainers != null) {
            // Get containers size
            val newestContainersSize = newestContainers.size

            // For each matrix in the page
            for (newestContainer <- newestContainers) {
              newestPcm = newestContainer.getPcm

              // Search for a matrix in the current page only if matrix inside
              if (oldestContainersSize >= 1) {
                oldestContainer = Option[PCMContainer](oldestContainers.get(0))
                if (oldestContainersSize > 1) {
                  // FIXME : A bug is present on opencompare while parsing multiple matrices inside the same section
                  oldestContainer = oldestContainers.find(container => container.getPcm.getName == newestPcm.getName)
                }

                // If matrix has not been found
                if (!oldestContainer.isDefined) {
                  // It could have a different name (newly created section or removed)
                  // so get its position on the oldest revision based on its actual position
                  val newestContainerIndex = newestContainers.indexOf(newestContainer)
                  try {
                    oldestContainer = Option(oldestContainers.apply(newestContainerIndex))
                  } catch {
                    case _: Exception => println
                  }
                }
                if (oldestContainer.isDefined) {
                  oldestPcm = oldestContainer.get.getPcm
                  addMetric(oldestId, newestId, oldestPcm, newestPcm, DateTime.parse(date), oldestContainersSize, newestContainersSize)
                } else {
                  logger.fatal(title + " -- " + newestId + " -- referenced matrix not found in revision " + oldestId)
                }
              } else {
                // By the miracle of time, matrix appears
                newMatrix(newestId, DateTime.parse(date), newestContainers)
                logger.debug(title + " -- " + newestId + " -- new page with " + newestContainersSize + " matrix(ces)")
              }
            }
          } else {
            // First line so new matrices
            newMatrix(oldestId, DateTime.parse(date), oldestContainers)
            logger.debug(title + " -- " + oldestId + " -- first page with " + oldestContainersSize + " matrix(ces)")
          }
          revisionsDone += 1
        }
        // The current (oldiest) container becomes the new container to compare
        // In short terms, next line ;)
        newestContainers = oldestContainers
      } catch {
        case e: Exception => {
          logger.error(title + " -- " + oldestId + " -- " + e.getLocalizedMessage)
          logger.error(e.getStackTraceString)
        }
      }
      // Next revision
      newestId = oldestId
    })
    // And return the results
    Map[String, Int](("revisionsDone", revisionsDone), ("revisionsSize", revisionsSize))
  }

  private def addMetric(oldId : Int, newId : Int, oldestPcm : PCM, newestPcm : PCM, date : DateTime, oldSize : Int, newSize : Int): Unit = {
    val diff = newestPcm.diff(oldestPcm, new ComplexePCMElementComparator)
    metrics.add(Map[String, Any](
      ("id", newId),
      ("name", newestPcm.getName),
      ("date", date),
      ("parentId", oldId),
      ("nbMatrices", newSize),
      ("diffMatrices", (newSize - oldSize)),
      ("newFeatures", diff.getFeaturesOnlyInPCM1.size()),
      ("delFeatures", diff.getFeaturesOnlyInPCM2.size()),
      ("newProducts", diff.getProductsOnlyInPCM1.size()),
      ("delProducts", diff.getProductsOnlyInPCM2.size()),
      ("changedCells", diff.getDifferingCells.size())
    ))
  }

  private def newMatrix(id : Int, date : DateTime, containers : List[PCMContainer]): Unit = {
    for (container <- containers) {
      val pcm = container.getPcm
      metrics.add(Map[String, Any](
        ("id", id),
        ("name", pcm.getName),
        ("date", date),
        ("parentId", 0),
        ("nbMatrices", containers.size),
        ("diffMatrices", containers.size),
        ("newFeatures", pcm.getConcreteFeatures.size()),
        ("delFeatures", 0),
        ("newProducts", pcm.getProducts.size()),
        ("delProducts", 0),
        ("changedCells", 0)
      ))
    }
  }

}
