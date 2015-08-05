package org.opencompare.stats.launchers

import org.apache.log4j.{FileAppender, Level, Logger}
import org.opencompare.io.wikipedia.io.MediaWikiAPI
import org.opencompare.stats.utils.DataBase

/**
 * Created by smangin on 23/07/15.
 */
class Metrics(api : MediaWikiAPI, db : DataBase, time : String, wikitextPath : String, appender : FileAppender, level : Level) {

  private val logger = Logger.getLogger("metrics")
  logger.addAppender(appender)
  logger.setLevel(level)

  def start(): Unit = {
    db.createTableMetrics()

    val revisions = db.getRevisions()
    val pages = revisions.groupBy(line => {
      line.get("title").get
    })
    var pageDone = synchronized[Int](1)
    var revisionDone = synchronized[Int](0)
    val pagesSize = pages.size
    logger.debug("Process => Nb. total pages: " + pagesSize)
    logger.debug("Provess => Nb. total revisions: " + revisions.size)
    val groupThread = new ThreadGroup("metrics")
    pages.foreach(page => {
      val title = page._1.toString
      val content = synchronized(page._2)
      val comparator = new RevisionsComparator(db, api, wikitextPath, appender, level)
      val thread = new Thread(groupThread, title) {
        override def run() {
          try {
            // your custom behavior here
            val result: Map[String, Int] = comparator.process(title, content)
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
              logger.error(title + " => " + e.getLocalizedMessage)
              logger.error(e.getStackTraceString)
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
    logger.debug("process finished.")
  }
}

