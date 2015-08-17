package org.opencompare.stats.processes

import org.apache.log4j.{FileAppender, Level, Logger}
import org.opencompare.io.wikipedia.io.MediaWikiAPI
import org.opencompare.stats.utils.{DatabaseSqlite, RevisionsComparator}

/**
 * Created by smangin on 23/07/15.
 */
class Metrics(api : MediaWikiAPI, db : DatabaseSqlite, time : String, wikitextPath : String, pcmPath : String, appender : FileAppender, level : Level) {

  val groupThread = new ThreadGroup("metrics")

  // Logging
  private val logger = Logger.getLogger("metrics")
  logger.addAppender(appender)
  logger.setLevel(level)

  def compute(): Unit = {
    logger.info("Started...")
    val revisions = db.browseRevisions()
    val pages = revisions.groupBy(line => {
      line.get("title").get
    })
    val authors = revisions.groupBy(line => {
      line.get("author").get
    })
    var pageDone = synchronized[Int](1)
    var revisionDone = synchronized[Int](0)
    val pagesSize = pages.size

    logger.debug("Nb. total pages: " + pagesSize)
    logger.debug("Nb. total revisions: " + revisions.size)
    logger.debug("Nb. authors: " + authors.size)
    pages.foreach(page => {
      val title = page._1.toString
      val content = synchronized(page._2)
      val comparator = new RevisionsComparator(db, api, wikitextPath, pcmPath, appender, level)
      val thread = new Thread(groupThread, title) {
        override def run() {
          val result: Map[String, Int] = comparator.compare(title, content)
          comparator.getMetrics().foreach(line => {
            db.createMetrics(line)
          })
          //logger.info(pageDone + "/" + pagesSize + "\t[" + result.apply("revisionsDone") + "/" + result.apply("revisionsSize") + "\trev.]\t" + title)
          revisionDone += result.apply("revisionsDone")
          pageDone += 1
        }
      }
      thread.join()
      thread.start()
      Thread.sleep(100) // slow the loop down a bit
    })
    logger.info("All threads started, waiting to finish...")
    while (groupThread.activeCount() > 0) {print(".")}
    logger.info("Nb. pages done (estimation): " + pageDone)
    logger.info("Nb. revisions done (estimation): " + revisionDone)
    logger.info("Waiting for database threads to terminate...")
    while (db.isBusy()) {print(".")}
    val done = db.browseMetrics()
    logger.info("Nb. comparisons done: " + done.size)
    logger.info("process finished.")
  }
}

