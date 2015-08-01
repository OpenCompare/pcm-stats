package org.opencompare.stats

import org.apache.log4j.Logger
import org.opencompare.api.java.util.{ComplexePCMElementComparator, DiffResult}
import org.opencompare.api.java.{PCM, PCMContainer}
import org.opencompare.io.wikipedia.io.{MediaWikiAPI, WikiTextLoader, WikiTextTemplateProcessor}

import scala.collection.JavaConversions._
import scala.io.Source

/**
 * Created by blacknight on 30/07/15.
 */
class PcmMetrics(db : DataBase, api : MediaWikiAPI, wikitextPath : String) extends Thread {

  private val wikiLoader = new WikiTextLoader(new WikiTextTemplateProcessor(api))

  def process(title : String, content : List[Map[String, Any]], logger : Logger): Map[String, Int] = {
    var previousId : Int = 0
    var currentId : Int = 0
    var currentContainer : Option[PCMContainer] = null
    var currentPcm : PCM = null
    var previousPcm : PCM = null
    var currentContainers : List[PCMContainer] = null
    var previousContainers : List[PCMContainer] = null
    var revisionsSize = content.size
    var revisionsDone = 0
    var wikitext = ""
    var diff : DiffResult = null

    // Sort by revision Id newer to older
    content.sortBy(line => line.apply("id").asInstanceOf[Int]).reverse.foreach(line => {
      val lang = line.get("lang").get.toString
      currentId = line.get("id").get.asInstanceOf[Int]
      //try {
        // Get the wikitext code
        wikitext = Source.fromFile(wikitextPath + title + "-" + currentId + ".wikitext").mkString
        // Parse it through wikipedia miner
        currentContainers = wikiLoader.mine(lang, wikitext, title).toList
        //  It should avoid multiple unnamed matrix
        if (currentContainers.count(container => container.getPcm.getName == (title + " -   ")) <= 1) {

          // If first line, go next
          if (previousContainers != null) {

            // Get containers size
            val currentContainersSize = currentContainers.size
            val previousContainersSize = previousContainers.size

            // For each matrix in the page
            for (previousContainer <- previousContainers) {
              previousPcm = previousContainer.getPcm

              // Search for a matrix in the current page only if more than 1 matrix inside
              // FIXME : if the matrix is a new one, the results won't be objective
              // cf: Comparison_of_Dewey_and_Library_of_Congress_subject_classification for matrix name change only through title
              if (currentContainersSize == 1) {
                currentContainer = Option[PCMContainer](currentContainers.get(0))
              } else {
                // FIXME : A bug is present while parsing multiple matrices inside the same section
                currentContainer = currentContainers.find(container => container.getPcm.getName == previousPcm.getName)
              }

              if (currentContainer.isDefined) {
                // If the matrix exists in the current line
                currentPcm = currentContainer.get.getPcm
                // Treatment by comparing previous line with current one to populate previous container line
                diff = currentPcm.diff(previousPcm, new ComplexePCMElementComparator)
                db.syncExecute("insert into metrics values(" +
                  currentId+", "+
                  "'"+currentPcm.getName.replace("'", "")+"', "+
                  previousId+", "+
                  currentContainersSize+", "+
                  previousContainersSize+", "+
                  (currentContainersSize - previousContainersSize)+", "+
                  diff.getFeaturesOnlyInPCM2.size()+", "+
                  diff.getFeaturesOnlyInPCM1.size()+", "+
                  diff.getProductsOnlyInPCM2.size()+", "+
                  diff.getProductsOnlyInPCM1.size()+", "+
                  diff.getDifferingCells.size()+")")
              //} else {
              //  // Otherwize populate metrics with the new matrix properties
              //  // FIXME : find a better way to shwo the difference
              //  db.syncExecute("insert into metrics values(" +
              //    currentId+", "+
              //    "'"+previousPcm.getName.replace("'", "")+"', "+
              //    previousId+", "+
              //    currentContainersSize+", "+
              //    previousContainersSize+", "+
              //    -1+", "+
              //    0+", "+
              //    0+", "+
              //    0+", "+
              //    0+", "+
              //    0+")")
              }
            }
          }
          revisionsDone += 1
        }
        // The current container becomes the new matrix to diff
        previousContainers = currentContainers
      //} catch {
      //  case e: Throwable => logger.error(e.toString)
      //}
      previousId = currentId
    })
    Map[String, Int](("revisionsDone", revisionsDone), ("revisionsSize", revisionsSize))
  }

}
