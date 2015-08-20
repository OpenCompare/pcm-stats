package org.opencompare.stats.processes

import org.apache.log4j.{FileAppender, Level, Logger}
import org.opencompare.io.wikipedia.io.MediaWikiAPI
import org.opencompare.stats.utils.{DatabaseSqlite, RevisionsComparator}
import scala.concurrent._
import ExecutionContext.Implicits.global


/**
 * Created by smangin on 23/07/15.
 */
class Metrics(api : MediaWikiAPI, db : DatabaseSqlite, time : String, wikitextPath : String, pcmPath : String, appender : FileAppender, level : Level) {


  // Logging
  private val logger = Logger.getLogger("metrics")
  logger.addAppender(appender)
  logger.setLevel(level)

  def compute(): Future[List[Int]] = {
    logger.info("Started...")
    val revisions = db.browseRevisions()
    val pages = revisions.groupBy(line => {
      line.get("title").get
    })
    val authors = revisions.groupBy(line => {
      line.get("author").get
    })

    val pagesSize = pages.size

    logger.debug("Nb. total pages: " + pagesSize)
    logger.debug("Nb. total revisions: " + revisions.size)
    logger.debug("Nb. authors: " + authors.size)


    val tasks = for (page <- pages) yield {
      Future {
        val title = page._1.toString
        val content = page._2
        val comparator = new RevisionsComparator(db, api, wikitextPath, pcmPath, appender, level)

        val result: Map[String, Int] = comparator.compare(title, content)
        comparator.getMetrics().foreach(line => {
          db.createMetrics(line)
        })
        //logger.info(pageDone + "/" + pagesSize + "\t[" + result.apply("revisionsDone") + "/" + result.apply("revisionsSize") + "\trev.]\t" + title)
        val revisionDone = result("revisionsDone")

        revisionDone
      }
    }

    val aggregatedTasks = Future.sequence(tasks.toList)

    aggregatedTasks

  }
}

