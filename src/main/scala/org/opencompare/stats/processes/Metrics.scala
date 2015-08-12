package org.opencompare.stats.processes

import org.apache.log4j.{FileAppender, Level, Logger}
import org.opencompare.io.wikipedia.io.MediaWikiAPI
import org.opencompare.stats.utils.{DatabaseSqlite, RevisionsComparator}

/**
 * Created by smangin on 23/07/15.
 */
class Metrics(api : MediaWikiAPI, db : DatabaseSqlite, time : String, wikitextPath : String, appender : FileAppender, level : Level) {

  private val logger = Logger.getLogger("metrics")
  logger.addAppender(appender)
  logger.setLevel(level)

  val groupThread = new ThreadGroup("metrics")

  val revisions = db.browseRevisions()
  val pages = revisions.groupBy(line => {
    line.get("title").get
  })
  var pageDone = synchronized[Int](1)
  var revisionDone = synchronized[Int](0)
  val pagesSize = pages.size

  def start(): Unit = {
    logger.debug("Nb. total pages: " + pagesSize)
    logger.debug("Nb. total revisions: " + revisions.size)
    pages.foreach(page => {
      val title = page._1.toString
      val content = synchronized(page._2)
      val comparator = new RevisionsComparator(api, wikitextPath, appender, level)
      val thread = new Thread(groupThread, title) {
        override def run() {
          try {
            // your custom behavior here
            val result: Map[String, Int] = comparator.compare(title, content)
            for (line : Map[String, Any] <- comparator.getMetrics()) {
              try {
                db.createMetrics(line)
              } catch {
                case e: Exception => {
                  logger.error(title + " -- " + line.apply("id").asInstanceOf[Int] + " -- " + e.getLocalizedMessage)
                  logger.error(e.getStackTraceString)
                }
              }
            }
            val log = pageDone + "/" + pagesSize + "\t[" + result.apply("revisionsDone") + "/" + result.apply("revisionsSize") + "\trev.]\t" + title
            if (result.apply("revisionsDone") == result.apply("revisionsSize")) {
              logger.info(log)
            } else {
              logger.warn(log)
            }
            pageDone += 1
            revisionDone += result.apply("revisionsDone").asInstanceOf[Int]
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
    logger.debug("Nb. pages done (estimation): " + pageDone)
    logger.debug("Nb. revisions done (estimation): " + revisionDone)
    logger.debug("Waiting for database threads to terminate...")
    while (db.isBusy()) {}
    val done = db.browseMetrics()
    logger.debug("Nb. comparisons done: " + done.size)
    logger.debug("process finished.")
  }
}

