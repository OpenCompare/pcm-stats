package org.opencompare.stats.processes

import java.io.{File, FileWriter}

import com.github.tototoshi.csv.CSVReader
import org.apache.log4j.{Level, FileAppender, Logger}
import org.joda.time.DateTime
import org.opencompare.io.wikipedia.io.MediaWikiAPI
import org.opencompare.stats.utils.{RevisionsParser, CustomCsvFormat, DatabaseSqlite}

/**
 * Created by smangin on 23/07/15.
 */
class Revisions(api : MediaWikiAPI, db : DatabaseSqlite, time : String, wikitextPath : String, appender : FileAppender, level : Level) {

  // File configurations
  private val inputPageList = new File("src/main/resources/list_of_PCMs.csv")
  private val reader = CSVReader.open(inputPageList)(new CustomCsvFormat)

  // Logging
  private val logger = Logger.getLogger("revisions")
  private val database_logger = Logger.getLogger("revisions.database")
  logger.addAppender(appender)
  logger.setLevel(level)
  database_logger.addAppender(appender)

  // Statistical vars
  private val pages = reader.allWithHeaders()
  private val groups = pages.grouped(10).toList // Performance issue hack
  private var pagesSize = pages.size
  private var pagesDone = synchronized(0)
  private var revisionsDone = synchronized(0)
  private var newRevisions = synchronized(0)
  private var delRevisions = synchronized(0)
  private val groupThread = new ThreadGroup("revisions")

  def start() {
    // Parse wikipedia page list
    groups.foreach(group => {
      var pageTitle = group.head.get("Title").get
      val thread = new Thread(groupThread, pageTitle) {
        override def run() {
          group.foreach(line => {
            val pageLang = line.get("Lang").get
            pageTitle = line.get("Title").get
            val file = wikitextPath + pageTitle.replace("'", "") + "/"
            new File(file).mkdirs()
            try {
              val revision = new RevisionsParser(api, pageLang, pageTitle, "older")
              val ids = revision.getIds(true, true)
              for (revid: Int <- ids) {
                // Keep an eye on already existing revisions
                val fileName = file + revid + ".wikitext"
                val revisionFile = new File(fileName)
                val parentId = revision.getParentId(revid)
                // To prevent from matrix deletion then addition,  delete the parentid from the database (it should be here because of the older to newer sorting)
                try {
                  db.insertRevision(Map(
                    ("id", revid),
                    ("title", pageTitle),
                    ("date", DateTime.parse(revision.getDate(revid).get)),
                    ("author", revision.getAuthor(revid)),
                    ("parentId", parentId),
                    ("lang", pageLang)
                  ))
                  revisionsDone += 1
                  newRevisions += 1
                } catch {
                  case e: Exception => {
                    database_logger.error(pageTitle + " -- " + revid + " -- " + e.getLocalizedMessage)
                    database_logger.error(e.getStackTraceString)
                  }
                }
                if (!revisionFile.exists() || revisionFile.length() == 0) {
                  // Save wikitext
                  var wikitext = ""
                  try {
                    wikitext = revision.getWikitext(revid)
                  } catch {
                    case e: Exception => {
                      logger.error(pageTitle + " -- " + revid + " -- " + e.getLocalizedMessage)
                      logger.error(e.getStackTraceString)
                    }
                  }
                  if (wikitext != "") {
                    val wikiWriter = new FileWriter(revisionFile)
                    wikiWriter.write(wikitext)
                    wikiWriter.close()
                    //logger.debug(pageTitle + " => '" + revid + "' wikitext retreived and saved") // Too much verbose
                  } else {
                    try {
                      db.deleteRevision(revid)
                      logger.warn(pageTitle + " -- " + revid + " -- " + " is a blank revision. deleted.")
                    } catch {
                      case e: Exception => {
                        database_logger.error(pageTitle + " -- " + revid + " -- " + e.getLocalizedMessage)
                        database_logger.error(e.getStackTraceString)
                      }
                    }
                  }
                }
              }
              pagesDone += 1
              logger.info(pagesDone + "/" + pagesSize + "\t[" + ids.size + "/" + revision.getIds().size + " rev." + "]\t" + pageTitle)
            } catch {
              case e: Exception => {
                logger.error(pageTitle + " => " + e.getLocalizedMessage)
                logger.error(e.getStackTraceString)
              }
            }
          })
        }
      }
      thread.join()
      thread.start()
      Thread.sleep(100) // slow the loop down a bit
    })
    logger.debug("All threads started...")
    while (groupThread.activeCount() > 0) {}
    logger.info("Nb. total pages (estimation): " + pagesSize)
    logger.info("Nb. pages done (estimation): " + pagesDone)
    logger.info("Nb. revisions done (estimation): " + revisionsDone)
    logger.info("Nb. new revisions (estimation): " + newRevisions)
    logger.debug("Waiting for database threads to terminate...")
    while (db.isBusy()) {}
    val dbRevisions = db.getRevisions()
    database_logger.info("Nb. pages: " + dbRevisions.groupBy(line => line.apply("title")).toList.size)
    database_logger.info("Nb. revisions: " + dbRevisions.size)
    logger.info("process finished.")
  }
}

