package org.opencompare.stats.utils

import org.apache.log4j.{FileAppender, Level, Logger}
import org.joda.time.DateTime
import org.opencompare.api.java.impl.PCMFactoryImpl
import org.opencompare.api.java.util.ComplexePCMElementComparator
import org.opencompare.api.java.{PCM, PCMContainer}
import org.opencompare.io.wikipedia.io.{MediaWikiAPI, WikiTextLoader}
import org.opencompare.stats.exceptions.NoParentException

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
class RevisionsComparator(db : DatabaseSqlite, api: MediaWikiAPI, wikitextPath: String, pcmPath: String, appender: FileAppender, level: Level) extends Thread {

  private val logger = Logger.getLogger("metrics.comparator")
  logger.addAppender(appender)
  logger.setLevel(level)
  private val wikiLoader = new WikiTextLoader(new WikiTextKeepTemplateProcessor(api))
  var metrics: ListBuffer[Map[String, Any]] = ListBuffer.empty

  def getMetrics(): List[Map[String, Any]] = {
    metrics.toList
  }

  def nameCleaner(title : String, name : String): String = {
    name.replaceFirst(title, "").toLowerCase.replaceAll(" ", "").replaceFirst("-", "")
  }

  def compare(title: String, revisions: List[Map[String, Any]]): Map[String, Int] = {
    metrics = ListBuffer.empty
    var revisionsDone = 0
    var revisionsSize = revisions.size
    var oldestPcm: PCM = null
    var newestPcm: PCM = null

    // Group by revision id to spare some miner access
    revisions.sortBy(revision => revision.apply("id").asInstanceOf[Int]).reverse.foreach(revision => {

      val newestId = revision.get("id").get.asInstanceOf[Int]
      val oldestId = revision.get("parentId").get.asInstanceOf[Int]
      val lang = revision.get("lang").get.toString
      val date = revision.get("date").get.toString

      // Get the current wikitext code
      val newestWikifile = Source.fromFile(wikitextPath + title + "/" + newestId + ".wikitext")
      val newestWikitext = newestWikifile.mkString
      newestWikifile.close()

      val newestContainers = wikiLoader.mine(lang, newestWikitext, title).toList

      // Get containers size
      val newestContainersSize = newestContainers.size

      try {
        // If no parent revision create new matrices
        if (oldestId == 0) {
          throw new NoParentException()
        }

        // Get the parent wikitext code
        val oldestWikifile = Source.fromFile(wikitextPath + title + "/" + oldestId + ".wikitext")
        val oldestWikitext = oldestWikifile.mkString
        oldestWikifile.close()

        // Then parse wikitext with the wikipedia miner
        val oldestContainers = wikiLoader.mine(lang, oldestWikitext, title).toList
        // Get parent containers size
        val oldestContainersSize = oldestContainers.size

        // Get if container has too much unnamed matrix
        val tooUnamedMatrices = newestContainers.count(container => nameCleaner(title, container.getPcm.getName) == "") > 1

        // Get containers size
        val newestContainersSize = newestContainers.size

        // For each matrix in the page
        for (newestContainer <- newestContainers) {
          newestPcm = newestContainer.getPcm
          var name = nameCleaner(title, newestPcm.getName)

          // Search for a matrix in the parent page only if not empty
          if (oldestContainersSize >= 1) {
            var oldestContainer = Option[PCMContainer](oldestContainers.get(0))
            if (oldestContainersSize > 1) {
              // FIXME : A bug is present on opencompare while parsing multiple matrices inside the same section => same name !
              // We have to cleaned up all the matrix name to prevent upper/lower case and space modification
              oldestContainer = oldestContainers.find(container => nameCleaner(title, container.getPcm.getName) == name)
            }

            // If matrix has not been found
            if (!oldestContainer.isDefined) {
              // It could have a different name (newly created section or removed)
              // so get its position on the oldest revision based on its actual position
              val newestContainerIndex = newestContainers.indexOf(newestContainer)
              try {
                oldestContainer = Option(oldestContainers.apply(newestContainerIndex))
              } catch {
                case _: Exception => Nil
              }
            }
            if (oldestContainer.isDefined) {
              oldestPcm = oldestContainer.get.getPcm
              addMetric(title, newestId, oldestId, newestPcm, oldestPcm, DateTime.parse(date), newestContainersSize, oldestContainersSize)
            } else {
              // New matrix or renamed !
              val factory = new PCMFactoryImpl
              val pcm = factory.createPCM()
              addMetric(title, newestId, oldestId, newestPcm, pcm, DateTime.parse(date), newestContainersSize, oldestContainersSize)
              logger.warn(newestPcm.getName + " -- " + newestId + " -- referenced matrix not found in revision " + oldestId)
            }
          } else {
            val factory = new PCMFactoryImpl
            val pcm = factory.createPCM()
            addMetric(title, newestId, oldestId, newestPcm, pcm, DateTime.parse(date), newestContainersSize, oldestContainersSize)
            logger.debug(title + " -- " + newestId + " -- page with a new matrix")
          }
        }
        revisionsDone += 1
      } catch {
        case e: NoParentException => {
          newMatrices(title, newestId, DateTime.parse(date), newestContainers)
          logger.debug(title + " -- " + newestId + " -- first page with " + newestContainersSize + " matrix(ces)")
        }
        case e: Exception => {
          logger.error(title + " -- parentId " + oldestId + " -- " + e.getLocalizedMessage)
          logger.error(e.getStackTraceString)
        }
      }
    })
    // And return the results
    Map[String, Int](("revisionsDone", revisionsDone), ("revisionsSize", revisionsSize))
  }

  private def addMetric (title : String, oldId: Int, newId: Int, oldestPcm: PCM, newestPcm: PCM, date: DateTime, oldSize: Int, newSize: Int): Unit = {
    val diff = newestPcm.diff (oldestPcm, new ComplexePCMElementComparator)
    metrics.add (Map[String, Any] (
    ("id", newId),
    ("name", nameCleaner(title, newestPcm.getName)),
    ("originalName", newestPcm.getName),
    ("date", date),
    ("parentId", oldId),
    ("nbMatrices", newSize),
    ("diffMatrices", (oldSize - newSize) ),
    ("newFeatures", diff.getFeaturesOnlyInPCM1.size () ),
    ("delFeatures", diff.getFeaturesOnlyInPCM2.size () ),
    ("newProducts", diff.getProductsOnlyInPCM1.size () ),
    ("delProducts", diff.getProductsOnlyInPCM2.size () ),
    ("changedCells", diff.getDifferingCells.size () )
    ) )
  }

  private def newMatrices (title : String, id: Int, date: DateTime, containers: List[PCMContainer] ): Unit = {
    for (container <- containers) {
      val pcm = container.getPcm
      metrics.add (Map[String, Any] (
      ("id", id),
      ("name", nameCleaner(title, pcm.getName)),
      ("originalName", pcm.getName),
      ("date", date),
      ("parentId", 0),
      ("nbMatrices", containers.size),
      ("diffMatrices", containers.size),
      ("newFeatures", pcm.getConcreteFeatures.size () ),
      ("delFeatures", 0),
      ("newProducts", pcm.getProducts.size () ),
      ("delProducts", 0),
      ("changedCells", 0)
      ) )
    }
  }

}
