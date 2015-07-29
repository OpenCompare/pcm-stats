package org.opencompare.stats

import java.io.{File, FileWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.github.tototoshi.csv.{CSVReader, CSVWriter, DefaultCSVFormat, QUOTE_ALL}
import org.opencompare.api.java.util.{ComplexePCMElementComparator, DiffResult}
import org.opencompare.api.java.{PCM, PCMContainer}
import org.opencompare.io.wikipedia.io.{MediaWikiAPI, WikiTextLoader, WikiTextTemplateProcessor}

import scala.collection.JavaConversions._
import scala.io.Source

/**
 * Created by smangin on 23/07/15.
 */
object CustomCsvFormat extends DefaultCSVFormat {
  override val delimiter = ','
  override val quoteChar = '"'
  override val quoting = QUOTE_ALL
  override val treatEmptyLineAsNil = true
}

object Launcher extends App {

  val cTime = LocalDateTime.now()
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  // Paths
  val path = "metrics/"
  val pcmPath = path + "pcm/"
  val revisionsPath = path + "revisions/"
  val wikitextPath = revisionsPath + "wikitext/"

  // Creates directories with leaf paths
  new File(pcmPath).mkdirs()
  new File(wikitextPath).mkdirs()

  // File configurations
  val inputPageList = new File("src/main/resources/list_of_PCMs.csv")
  val outputRevisionsCsv = new File(revisionsPath + "revisionsList.csv")
  val outputPCMMetrics = new File(pcmPath + cTime.format(formatter) + "_pcmMetrics.csv")

  // Parsers
  val api: MediaWikiAPI = new MediaWikiAPI("https", "wikipedia.org")
  val wikiLoader = new WikiTextLoader(new WikiTextTemplateProcessor(api))

  def getRevisions(input : File, output : File) {
    println("###################################            Get Revisions               ##############################")
    // Stop if exists
    if (output.exists()) {
      println("Already done")
      return
    }
    println("Starting ...")
    output.createNewFile()
    val writer = CSVWriter.open(output)(CustomCsvFormat)
    // Heading
    val heading = List(
      "Title",
      "Id",
      "Date",
      "Lang",
      "Author"
    )
    writer.writeRow(heading)

    // Parse wikipedia page list
    val reader = CSVReader.open(input)(CustomCsvFormat)
    reader.allWithHeaders().foreach(line => {
      val pageLang = line.get("Lang").get
      val pageTitle = line.get("Title").get
      print(" > " + pageTitle)
      val revision = new Revision(api, pageLang, pageTitle)
      var revisionsSize = 0
      for (revid : Int <- revision.getIds) {
        writer.writeRow(List(
          revision.getTitle,
          revid,
          revision.getDate(revid).get,
          revision.getLang,
          revision.getAuthor(revid)
        ))
        writer.flush() // Save data at all costs
        // Save wikitext
        val wikiWriter = new FileWriter(new File(wikitextPath + revision.getTitle + "-" + revid + ".wikitext"))
        wikiWriter.write(revision.getWikitext(revid))
        wikiWriter.flush()
        wikiWriter.close()
        revisionsSize += 1
      }
      println(" => " + revisionsSize + " revisions")
    })
    writer.flush()
    writer.close()
  }

  def getPCMMetrics(input : File, output : File) {
    println("###################################           Get PCM metrics              ##############################")
    // Stop if exists
    if (output.exists()) {
      println("Already done")
      return
    }
    println("Starting ...")
    output.createNewFile()
    val reader = CSVReader.open(input)(CustomCsvFormat)
    val pages = reader.allWithHeaders().groupBy(line => {
      line.get("Title").get
    })
    val writer = CSVWriter.open(output)(CustomCsvFormat)
    // Heading
    val heading = List(
      "MatrixName",
      "Id",
      "PrevId",
      "Nbmatrices",
      "PrevNbmatrices",
      "Changedmatrices",
      "NewFeatures",
      "DelFeatures",
      "NewProducts",
      "DelProducts",
      "ChangedCells"
    )
    writer.writeRow(heading)

    println("Pages to process : " + pages.size)
    var remaining = pages.size
    pages.foreach(page => {
      // Local vars
      val title = page._1
      println("+ Page : " + title)
      var previousId : Int = 0
      var currentId : Int = 0
      var currentContainer : Option[PCMContainer] = null
      var currentPcm : PCM = null
      var previousPcm : PCM = null
      var currentContainers : List[PCMContainer] = null
      var previousContainers : List[PCMContainer] = null
      var revisionsSize = 0
      var wikitext = ""
      var diff : DiffResult = null

      // Sort by revision Id newer to older
      val content = page._2.sortBy(line => line.apply("Id").toInt).reverse
      page._2.foreach(line => {
        val lang = line.get("Lang").get
        currentId = line.get("Id").get.toInt

        try {
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
                  diff = previousPcm.diff(currentPcm, new ComplexePCMElementComparator)
                  writer.writeRow(List(
                    previousPcm.getName,
                    previousId,
                    currentId,
                    previousContainersSize,
                    currentContainersSize,
                    previousContainersSize - currentContainersSize,
                    diff.getFeaturesOnlyInPCM1.size(),
                    diff.getFeaturesOnlyInPCM2.size(),
                    diff.getProductsOnlyInPCM1.size(),
                    diff.getProductsOnlyInPCM2.size(),
                    diff.getDifferingCells.size()
                  ))
                } else {
                  // Otherwize populate metrics with the new matrix properties
                  // FIXME : find a better way to shwo the difference
                  writer.writeRow(List(
                    previousPcm.getName,
                    previousId,
                    currentId,
                    previousContainersSize,
                    currentContainersSize,
                    -1,
                    0,
                    0,
                    0,
                    0,
                    0
                  ))
                }
              }
            }
          } else {
            println("Too many unnamed matrices in container")
          }
          // The current container becomes the new matrix to diff
          previousContainers = currentContainers
        } catch {
          case e: Throwable => println(e)
        }
        previousId = currentId
        revisionsSize += 1
      })
      writer.flush()
      println(" => " + revisionsSize + " revisions")
      remaining = remaining - 1
      println(remaining + " remaining pages ")
    })
    writer.close()
  }

  def getWikitextMetrics(input : File): Unit = {
    val reader = CSVReader.open(input)(CustomCsvFormat)
    reader.all().foreach(line => {
      val revid = line.apply(0)
      val date = line.apply(1)
      val pageTitle = line.apply(2)
      val pageLang = line.apply(3)
    })
  }

  // First get revisions list
  getRevisions(inputPageList, outputRevisionsCsv)

  // Then parse it to make metrics
  getPCMMetrics(outputRevisionsCsv, outputPCMMetrics)

  // And parse wikitext to make metrics
  //getWikitextMetrics(outputRevisionsCsv)

}
