package org.opencompare.stats

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.{FileAppender, Logger}
import org.opencompare.io.wikipedia.io.MediaWikiAPI
import org.opencompare.stats.utils.{DataBase, MetricsGenerator, CustomLoggerLayout}

object LauncherMetrics extends App {
  // Time
  val cTime = LocalDateTime.now()
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  // Paths
  val path = "metrics/"
  val wikitextPath = path + "wikitext/"
  // Logger
  val fh = new FileAppender(new CustomLoggerLayout(), path + "process.log")
  // Parser
  val api: MediaWikiAPI = synchronized(new MediaWikiAPI("https", "wikipedia.org"))
  // Database
  val db = new DataBase(path + cTime.format(formatter) +".db")

  if (new File(wikitextPath).exists()) {
    db.createTableMetrics()

    val logger = Logger.getLogger("pcm_metrics")
    logger.addAppender(fh)
    val revisions = db.getRevisions()
    val pages = revisions.groupBy(line => {
      line.get("title").get
    })
    logger.debug("Nb. pages to process: " + pages.size)
    logger.debug("Nb. revisions to process: " + revisions.size)
    logger.debug("Nb. user: " + revisions.groupBy(line => {
      line.get("author").get
    }).size)
    var pageDone = synchronized[Int](1)
    var revisionDone = synchronized[Int](0)
    val pagesSize = pages.size
    val groupThread = new ThreadGroup("metrics")
    pages.foreach(page => {
      val title = page._1.toString
      val content = synchronized(page._2)
      val metrics = new MetricsGenerator(db, api, wikitextPath)
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
            revisionDone += result.apply("revisionsDone")
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
    logger.debug("Waiting for threads to terminate...")
    while (groupThread.activeCount() > 0) {}
    logger.debug("Process => Nb. pages done: " + pageDone)
    logger.debug("Provess => Nb. revisions done: " + revisionDone)
    logger.debug("Waiting for database threads to terminate...")
    while (db.hasThreadsLeft()) {}
    val done = db.getMetrics()
    logger.debug("Database => Nb. revisions done: " + done.size)
  }
}

