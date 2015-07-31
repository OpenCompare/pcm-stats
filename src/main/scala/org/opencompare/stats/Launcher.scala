package org.opencompare.stats

import java.io.{File, FileWriter}
import java.sql.{Statement, SQLException, Connection, DriverManager}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import org.apache.log4j.{FileAppender, Logger, SimpleLayout}
import org.opencompare.io.wikipedia.io.MediaWikiAPI

object Launcher extends App {
  // Time
  val cTime = LocalDateTime.now()
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val fullFormatter = DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/yyyy")

  // Paths
  val path = "metrics/"
  val pcmPath = path + "pcm/"
  val revisionsPath = path + "revisions/"
  val wikitextPath = revisionsPath + "wikitext/"
  val outputPCMMetrics = pcmPath + cTime.format(formatter) + "/"
  // Creates directories with leaf paths
  new File(outputPCMMetrics).mkdirs()
  new File(wikitextPath).mkdirs()

  // Logger
  val fh = new FileAppender(new SimpleLayout(), path + cTime.format(formatter) + "_process.log")

  // Parsers
  val api: MediaWikiAPI = synchronized(new MediaWikiAPI("https", "wikipedia.org"))

  // File configurations
  val inputPageList = new File("src/main/resources/list_of_PCMs.csv")
  val outputRevisionsCsv = new File(revisionsPath + "revisionsList.csv")

  def getConnection(): Statement = {

    // load the sqlite-JDBC driver using the current class loader
    Class.forName("org.sqlite.JDBC");

    var connection : Connection = null;
    try {
      // create a database connection
      connection = DriverManager.getConnection("jdbc:sqlite:sample.db");
      val statement = connection.createStatement();
      statement.setQueryTimeout(30);  // set timeout to 30 sec.
       }
    catch {
      case e : SQLException => {
      // if the error message is "out of memory",
      // it probably means no database file is found
      println(e.getMessage());
    }
  }

  def getRevisions(input : File, output : File, grouped : Int = 10) {
    val logger = Logger.getLogger("revisions")
    logger.addAppender(fh)
    logger.info("Get Revisions")
    logger.debug("Started at " + LocalDateTime.now().format(fullFormatter))
    // Stop if exists
    if (output.exists()) {
      logger.info("Already done")
      return
    }
    output.createNewFile()
    val writer = synchronized(CSVWriter.open(output)(new CustomCsvFormat))
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
    val reader = CSVReader.open(input)(new CustomCsvFormat)
    var done = synchronized(1)
    val pages = reader.allWithHeaders()
    val groups = pages.grouped(grouped).toList // Performance issue hack
    var pagesSize = pages.size
    logger.info("Nb. pages: " + pages.size)
    val groupThread = new ThreadGroup("revisions")
    groups.foreach(group => {
      var pageTitle = group.head.get("Title").get
      val thread = new Thread(groupThread, pageTitle) {
        override def run() {
            group.foreach(line => {
              val pageLang = line.get("Lang").get
              pageTitle = line.get("Title").get
              try {
                // your custom behavior here
                val revision = new Revision(api, pageLang, pageTitle)
                var revisionsSize = revision.getIds().size
                for (revid: Int <- revision.getIds()) {
                  writer.writeRow(List(
                    revision.getTitle,
                    revid,
                    revision.getDate(revid).get,
                    revision.getLang,
                    revision.getAuthor(revid)
                  ))
                  // Save wikitext
                  val wikiWriter = new FileWriter(new File(wikitextPath + revision.getTitle + "-" + revid + ".wikitext"))
                  wikiWriter.write(revision.getWikitext(revid))
                  wikiWriter.close()
                }
                logger.info(done + "/" + pagesSize + "\t[" + revisionsSize + " rev." + "]\t" + pageTitle)
                done += 1
              } catch {
                case e: Throwable => {
                  logger.fatal(pageTitle + " => " + e)
                }
              }
            })
        }
      }
      thread.join()
      thread.start()
      Thread.sleep(100) // slow the loop down a bit
    })
    while (groupThread.activeCount() > 0) {}
    writer.close()
    logger.debug("Ended at " + LocalDateTime.now().format(fullFormatter))
  }

  def getPCMMetrics(input : File, output : String) {
    val logger = Logger.getLogger("pcm_metrics")
    logger.addAppender(fh)
    logger.info("Get PCM metrics")
    logger.debug("Started at " + LocalDateTime.now().format(fullFormatter))
    val reader = CSVReader.open(input)(new CustomCsvFormat)
    val revisions = reader.allWithHeaders()
    val pages = revisions.groupBy(line => {
      line.get("Title").get
    })
    logger.info("Nb. pages: " + pages.size)
    logger.info("Nb. revisions: " + revisions.size)
    logger.info("Nb. user: " + revisions.groupBy(line => {
      line.get("Author").get
    }).size)
    var pageDone = synchronized[Int](1)
    val pagesSize = pages.size
    val groupThread = new ThreadGroup("metrics")
    pages.foreach(page => {
      val title = page._1
      val content = synchronized(page._2.sortBy(line => line.apply("Id").toInt).reverse)
      val metrics = new PcmMetrics(api, wikitextPath)
      val thread = new Thread(groupThread, title) {
        override def run() {
          try {
            // your custom behavior here
            val result : Map[String, Int] = metrics.process(title, content, output, logger)
            val log = pageDone + "/" + pagesSize + "\t[" + result.apply("revisionsDone") + "/" + result.apply("revisionsSize") + "\trev.]\t" + title
            if (result.apply("revisionsDone") == result.apply("revisionsSize")) {
              logger.info(log)
            } else {
              logger.warn(log)
            }
            pageDone += 1
          } catch {
            case e: Throwable => {
              logger.fatal(title + " => " + e)
              Thread.currentThread().interrupt()
              while (!Thread.currentThread().isInterrupted) {
                println(Thread.currentThread().getState)
              }
              Thread.currentThread().start()
            }
          }
        }
      }
      thread.join()
      thread.start()
      Thread.sleep(100) // slow the loop down a bit
    })
    while (groupThread.activeCount() > 0) {}
    logger.debug("Ended at " + LocalDateTime.now().format(fullFormatter))
  }

  def getWikitextMetrics(input : File): Unit = {
    val reader = CSVReader.open(input)(new CustomCsvFormat)
    reader.all().foreach(line => {
      val revid = line.head
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

