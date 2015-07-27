package org.opencompare.stats

import java.io.{File, FileWriter}

import com.github.tototoshi.csv.{CSVReader, CSVWriter, DefaultCSVFormat, QUOTE_ALL}
import org.opencompare.api.java.PCMContainer
import org.opencompare.api.java.util.ComplexePCMElementComparator
import org.opencompare.io.wikipedia.io.{MediaWikiAPI, WikiTextLoader, WikiTextTemplateProcessor}

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

  def getRevisions(input : File, output : File): Unit = {
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
    println("Heading is : " + heading)
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
  }

  def getPCMMetrics(input : File, output : File): Unit = {
    println("###################################        Get PCM metrics                 ##############################")
    val reader = CSVReader.open(input)(CustomCsvFormat)
    val pages = reader.allWithHeaders().groupBy(line => {
      line.get("Title").get
    })
    val writer = CSVWriter.open(output)(CustomCsvFormat)
    // Heading
    val heading = List(
      "Id",
      "PrevId",
      "NewFeatures",
      "DelFeatures",
      "NewProducts",
      "DelProducts",
      "ChangedCells"
    )
    println("Heading is : " + heading)
    writer.writeRow(heading)

    // Parse
    pages.foreach(page => {
      // Local vars
      val title = page._1
      println("+ Page : " + title)
      var previousId : Int = 0
      var currentId : Int = 0
      var currentContainer : PCMContainer = null
      var previousContainer : PCMContainer = null
      var revisionsSize = 0

      // Parse
      page._2.foreach(line => {
        val lang = line.get("Lang").get
        currentId = line.get("Id").get.toInt
        print("\t- " + currentId + "\t-> ")
        val wikitext = Source.fromFile(wikitextPath + title + "-" + currentId + ".wikitext").mkString
        try {
          currentContainer = wikiLoader.mine(lang, wikitext, title).get(0)
          if (previousContainer != null) {
            // Treatment by comparing previous line with current one to populate previous container line
            val diff = previousContainer.getPcm.diff(currentContainer.getPcm, new ComplexePCMElementComparator)
            writer.writeRow(List(
              previousId,
              currentId,
              diff.getFeaturesOnlyInPCM1.size(),
              diff.getFeaturesOnlyInPCM2.size(),
              diff.getProductsOnlyInPCM1.size(),
              diff.getProductsOnlyInPCM2.size(),
              diff.getDifferingCells.size()
            ))
            writer.flush()
            println("Done")
          }
          // After treatment
          previousContainer = currentContainer
        } catch {
          case e: Throwable => println(e)
        }
        previousId = currentId
        revisionsSize += 1
      })
      writer.close()
      println(" => " + revisionsSize + " revisions")
    })
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

  // And parse wiitext to make metrics
  //getWikitextMetrics(outputRevisionsCsv)
}
