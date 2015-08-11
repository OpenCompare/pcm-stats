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
  var metrics : List[Map[String, Any]] = _

  private val wikiLoader = new WikiTextLoader(new WikiTextKeepTemplateProcessor(api))

  def getMetrics(): List[Map[String, Any]] = {
    metrics
  }

  def compare(title : String, content : List[Map[String, Any]]): Map[String, Int] = {
    val currentMetrics : ListBuffer[Map[String, Any]]= ListBuffer()
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
                    case _: Exception => {
                      logger.fatal(title + " -- " + oldestId + " -- matrix not found")
                    }
                  }
                }
              }

              // If at least one matrix has been found
              if (oldestContainer.isDefined) {
                // If the matrix exists in the current line
                oldestPcm = oldestContainer.get.getPcm
                val diff = newestPcm.diff(oldestPcm, new ComplexePCMElementComparator)
                currentMetrics.add(Map[String, Any](
                  ("id", newestId),
                  ("name", newestPcm.getName),
                  ("date", DateTime.parse(date)),
                  ("parentId", oldestId),
                  ("nbMatrices", newestContainersSize),
                  ("diffMatrices", (newestContainersSize - oldestContainersSize)),
                  ("newFeatures", diff.getFeaturesOnlyInPCM1.size()),
                  ("delFeatures", diff.getFeaturesOnlyInPCM2.size()),
                  ("newProducts", diff.getProductsOnlyInPCM1.size()),
                  ("delProducts", diff.getProductsOnlyInPCM2.size()),
                  ("changedCells", diff.getDifferingCells.size())
                ))
              }
            }
          } else {
            logger.debug(title + " -- " + oldestId + " -- new page with " + oldestContainersSize + " matrix(ces)")
            // First line so new matrices
            for (container <- oldestContainers) {
              oldestPcm = container.getPcm
              currentMetrics.add(Map[String, Any](
                ("id", oldestId),
                ("name", oldestPcm.getName),
                ("date", DateTime.parse(date)),
                ("parentId", 0),
                ("nbMatrices", oldestContainersSize),
                ("diffMatrices", oldestContainersSize),
                ("newFeatures", oldestPcm.getConcreteFeatures.size()),
                ("delFeatures", 0),
                ("newProducts", oldestPcm.getProducts.size()),
                ("delProducts", 0),
                ("changedCells", 0)
              ))
            }
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
    // Append the lines to create
    metrics = currentMetrics.toList
    // And return the results
    Map[String, Int](("revisionsDone", revisionsDone), ("revisionsSize", revisionsSize))
  }
}
