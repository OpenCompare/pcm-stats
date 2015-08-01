package org.opencompare.stats

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.{FileAppender, Logger, SimpleLayout}
import org.opencompare.io.wikipedia.io.MediaWikiAPI

object LauncherMetrics extends App {
  // Time
  val cTime = LocalDateTime.now()
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val fullFormatter = DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/yyyy")
  // Paths
  val path = "revisions/"
  val wikitextPath = path + "wikitext/"
  // Logger
  val fh = new FileAppender(new SimpleLayout(), path + cTime.format(formatter) + "_process.log")
  // Parser
  val api: MediaWikiAPI = synchronized(new MediaWikiAPI("https", "wikipedia.org"))
  // Database
  val db = new DataBase(path + cTime.format(formatter) +".db")

  if (new File(wikitextPath).exists()) {
    db.createTableMetrics()

    val logger = Logger.getLogger("pcm_metrics")
    logger.addAppender(fh)
    logger.debug("Started at " + LocalDateTime.now().format(fullFormatter))
    val revisions = db.getRevisions()
    val pages = revisions.groupBy(line => {
      line.get("title").get
    })
    logger.debug("Nb. pages: " + pages.size)
    logger.debug("Nb. revisions: " + revisions.size)
    logger.debug("Nb. user: " + revisions.groupBy(line => {
      line.get("author").get
    }).size)
    var pageDone = synchronized[Int](1)
    val pagesSize = pages.size
    val groupThread = new ThreadGroup("metrics")
    pages.foreach(page => {
      val title = page._1.toString
      val content = synchronized(page._2)
      val metrics = new PcmMetrics(db, api, wikitextPath)
      val thread = new Thread(groupThread, title) {
        override def run() {
          try {
            // your custom behavior here
            val result: Map[String, Int] = metrics.process(title, content, logger)
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
}

