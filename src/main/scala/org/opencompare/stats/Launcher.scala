package org.opencompare.stats

import java.io.{File, FileWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.github.tototoshi.csv.CSVReader
import org.apache.log4j.{FileAppender, Logger, SimpleLayout}
import org.opencompare.io.wikipedia.io.MediaWikiAPI

object Launcher extends App {
  // Time
  val cTime = LocalDateTime.now()
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val fullFormatter = DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/yyyy")
  // File configurations
  val inputPageList = new File("src/main/resources/list_of_PCMs.csv")
  // Paths
  val path = "revisions/"
  val wikitextPath = path + "wikitext/"
  // Creates directories with leaf paths
  new File(wikitextPath).mkdirs()
  // Logger
  val fh = new FileAppender(new SimpleLayout(), path + cTime.format(formatter) + "_process.log")
  // Parser
  val api: MediaWikiAPI = synchronized(new MediaWikiAPI("https", "wikipedia.org"))
  // Database
  val db = new DataBase(path + cTime.format(formatter) +".db")

  def getRevisions(input : File, grouped : Int = 10) {
    val logger = Logger.getLogger("revisions")
    logger.addAppender(fh)
    logger.info("Get Revisions")
    logger.debug("Started at " + LocalDateTime.now().format(fullFormatter))

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
                  val sql : String = "insert into revisions values("+revid+", "+ "'"+revision.getTitle(revid)+"', "+ "'"+revision.getDate(revid).get+"', "+ "'"+revision.getLang(revid)+"', "+ "'"+revision.getAuthor(revid)+"')"
                  println(sql)
                  db.statement.execute(sql)
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
    logger.debug("Ended at " + LocalDateTime.now().format(fullFormatter))
  }

  def getPCMMetrics() {
    val logger = Logger.getLogger("pcm_metrics")
    logger.addAppender(fh)
    logger.info("Get PCM metrics")
    logger.debug("Started at " + LocalDateTime.now().format(fullFormatter))
    val revisions = db.getRevisions()
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
      val title = page._1.toString
      val content = synchronized(page._2.sortBy(line => line.apply("Id").asInstanceOf[Int]).reverse)
      val metrics = new PcmMetrics(db, api, wikitextPath)
      val thread = new Thread(groupThread, title) {
        override def run() {
          try {
            // your custom behavior here
            val result : Map[String, Int] = metrics.process(title, content, logger)
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
  getRevisions(inputPageList)

  // Then parse it to make metrics
  getPCMMetrics()

  // And parse wikitext to make metrics
  //getWikitextMetrics(outputRevisionsCsv)

}

