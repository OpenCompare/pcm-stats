package org.opencompare.stats

import java.io.{File, FileWriter}
import java.util.Calendar

import com.github.tototoshi.csv.{CSVReader, CSVWriter, DefaultCSVFormat, QUOTE_ALL}
import org.opencompare.api.java.util.{DiffResult, ComplexePCMElementComparator}
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

  // Paths
  val path = "metrics/"
  val wikitextPath = path + "wikitext/"

  // Creates directories with leaf paths
  new File(wikitextPath).mkdirs()

  // File configurations
  val inputPageList = new File("src/main/resources/list_of_PCMs.csv")
  val outputRevisionsCsv = new File(path + "revisionsList.csv")
  val outputPCMMetrics = new File(path + "pcmMetrics.csv")

  // Parsers
  val api: MediaWikiAPI = new MediaWikiAPI("https", "wikipedia.org")
  val wikiLoader = new WikiTextLoader(new WikiTextTemplateProcessor(api))

  def getRevisions(input : File, output : File) {
    val start = Calendar.getInstance().getTime
    println("###################################          Get Revisions                 ##############################")
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
      val revision = new Revision(api, pageLang, pageTitle)
      println(" + Page : " + pageTitle)
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
    println((Calendar.getInstance().getTime.getTime - start.getTime)/60 + "min elapsed time")
  }

  def getPCMMetrics(input : File, output : File) {
    val start = Calendar.getInstance().getTime
    println("###################################        Get PCM metrics                 ##############################")
    val reader = CSVReader.open(input)(CustomCsvFormat)
    val pages = reader.allWithHeaders().groupBy(line => {
      line.get("Title").get
    })
    val writer = CSVWriter.open(output)(CustomCsvFormat)
    // Heading
    val heading = List(
      "Matrix",
      "Id",
      "PrevId",
      "Nbmatrix",
      "NewFeatures",
      "DelFeatures",
      "NewProducts",
      "DelProducts",
      "ChangedCells"
    )
    writer.writeRow(heading)

    println("Pages to process : " + pages.size)
    var remaining = pages.size
    // Parse
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

      // Parse
      page._2.foreach(line => {
        val lang = line.get("Lang").get
        currentId = line.get("Id").get.toInt
        // Get the wikitext code
        wikitext = Source.fromFile(wikitextPath + title + "-" + currentId + ".wikitext").mkString
        try {
          // Parse it throught wikipedia miner
          currentContainers = wikiLoader.mine(lang, wikitext, title).toList
          // If first line, go next
          if (previousContainers != null) {
            // Fir each matrix in the page
            for (previousContainer <- previousContainers) {
              previousPcm = previousContainer.getPcm
              // Search for a matrix in the current page
              currentContainer = currentContainers.find(container => container.getPcm.getName == previousPcm.getName)
              // If the matrix exists in the current line
              if (currentContainer.isDefined) {
                currentPcm = currentContainer.get.getPcm
                // Treatment by comparing previous line with current one to populate previous container line
                diff = previousPcm.diff(currentPcm, new ComplexePCMElementComparator)
                writer.writeRow(List(
                  previousPcm.getName,
                  previousId,
                  currentId,
                  previousContainers.size,
                  diff.getFeaturesOnlyInPCM1.size(),
                  diff.getFeaturesOnlyInPCM2.size(),
                  diff.getProductsOnlyInPCM1.size(),
                  diff.getProductsOnlyInPCM2.size(),
                  diff.getDifferingCells.size()
                ))
              } else {
                // Otherwize populate metrics with the new matrix properties
                writer.writeRow(List(
                  previousPcm.getName,
                  previousId,
                  currentId,
                  previousContainers.size,
                  previousPcm.getConcreteFeatures.size(),
                  0,
                  previousPcm.getProducts.size(),
                  0,
                  previousPcm.getProducts.size() * previousPcm.getConcreteFeatures.size()
                ))
              }
            }
          }
          // After treatment
          previousContainers = currentContainers
        } catch {
          case e: Throwable => Nil
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
    println((Calendar.getInstance().getTime.getTime - start.getTime)/60 + "min elapsed time")
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
