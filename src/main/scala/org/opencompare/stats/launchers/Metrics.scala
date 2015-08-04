package org.opencompare.stats.launchers

import java.io.File

import org.apache.log4j.{FileAppender, Logger}
import org.opencompare.io.wikipedia.io.MediaWikiAPI
import org.opencompare.stats.utils.{DataBase, MetricsComparator}

class Metrics(api : MediaWikiAPI, db : DataBase, time : String, wikitextPath : String, appender : FileAppender) {

  private val logger = Logger.getLogger("metrics")
  logger.addAppender(appender)

  def start(): Unit = {
    if (new File(wikitextPath).exists()) {
      db.createTableMetrics()

      val revisions = db.getRevisions()
      val pages = revisions.groupBy(line => {
        line.get("title").get
      })
      var pageDone = synchronized[Int](1)
      var revisionDone = synchronized[Int](0)
      val pagesSize = pages.size
      val groupThread = new ThreadGroup("metrics")
      pages.foreach(page => {
        val title = page._1.toString
        val content = synchronized(page._2)
        val metrics = new MetricsComparator(db, api, wikitextPath, appender)
        val thread = new Thread(groupThread, title) {
          override def run() {
            try {
              // your custom behavior here
              val result: Map[String, Int] = metrics.process(title, content)
              val log = pageDone + "/" + pagesSize + "\t[" + result.apply("revisionsDone") + "/" + result.apply("revisionsSize") + "\trev.]\t" + title
              if (result.apply("revisionsDone") == result.apply("revisionsSize")) {
                logger.info(log)
              } else {
                logger.warn(log)
              }
              pageDone += 1
              revisionDone += result.apply("revisionsDone")
            } catch {
              case e: Exception => {
                logger.error(title + " => " + e.getStackTraceString)
              }
            }
          }
        }
        thread.join()
        thread.start()
        Thread.sleep(100) // slow the loop down a bit
      })
      logger.debug("All threads started...")
      while (groupThread.activeCount() > 0) {}
      logger.debug("Process => Nb. pages done: " + pageDone)
      logger.debug("Provess => Nb. revisions done: " + revisionDone)
      logger.debug("Waiting for database threads to terminate...")
      while (db.hasThreadsLeft()) {}
      val done = db.getMetrics()
      logger.debug("Database => Nb. comparisons done: " + done.size)
    }
    logger.debug("process finished.")
  }
}

