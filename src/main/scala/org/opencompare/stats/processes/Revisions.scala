package org.opencompare.stats.processes

import java.io.{File, FileWriter}

import com.github.tototoshi.csv.CSVReader
import org.apache.log4j.{FileAppender, Level, Logger}
import org.joda.time.DateTime
import org.opencompare.io.wikipedia.io.MediaWikiAPI
import org.opencompare.stats.utils.{CustomCsvFormat, DatabaseSqlite, RevisionsParser}

/**
 * Created by smangin on 23/07/15.
 */
class Revisions(api : MediaWikiAPI, db : DatabaseSqlite, time : String, wikitextPath : String, appender : FileAppender, level : Level) {

  // File configurations
  private val inputPageList = new File("src/main/resources/list_of_PCMs.csv")
  private val reader = CSVReader.open(inputPageList)(new CustomCsvFormat)
  val groupThread = new ThreadGroup("revisions")

  // Logging
  private val logger = Logger.getLogger("revisions")
  private val database_logger = Logger.getLogger("revisions.database")
  logger.addAppender(appender)
  logger.setLevel(level)
  database_logger.addAppender(appender)

  def compute(groupBy : Int) {
    val pages = reader.allWithHeaders()
    val groups = pages.grouped(groupBy).toList // Performance issue hack
    // Statistical vars
    var pagesSize = pages.size
    var pagesDone = synchronized(0)
    var revisionsDone = synchronized(0)
    var revisionsSize = synchronized(0)
    var revisionsUndo = synchronized(0)
    var revisionsBlank = synchronized(0)
    var revisionsNew = synchronized(0)
    var revisionsDel = synchronized(0)

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
              revisionsSize += ids.apply("ids").size
              revisionsUndo += ids.apply("undo").size
              revisionsBlank += ids.apply("blank").size
              revisionsDel += ids.apply("suppressed").size
              for (revid: Int <- ids.apply("ids")) {

                val fileName = file + revid + ".wikitext"
                val revisionFile = new File(fileName)
                val parentId = revision.getParentId(revid)
                // Keep an eye on already existing revisions
                if (!db.revisionExists(revid)) {
                  try {
                    db.createRevision(Map(
                      ("id", revid),
                      ("title", pageTitle),
                      ("date", DateTime.parse(revision.getDate(revid).get)),
                      ("author", revision.getAuthor(revid)),
                      ("parentId", parentId),
                      ("lang", pageLang)
                    ))
                    revisionsNew += 1
                  } catch {
                    case e: Exception => {
                      database_logger.error(pageTitle + " -- " + revid + " -- " + e.getLocalizedMessage)
                      database_logger.error(e.getStackTraceString)
                    }
                  }
                } else {
                  revisionsDone += 1
                }
                // Get the wikitext if not in the folder or empty
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
                  // Manage undo empty revisions which has not been indicated by the revision parser(based on wikipedia metadata)
                  if (wikitext == "") {
                    try {
                      // We change the parent revision of the parent revision with the revision which is the parent revision of the current revision... Gniark gniark
                      var revidToCompareWith = 0
                      for (revId2 <- revision.getIds().apply("ids")) {
                        if (revision.getParentId(revId2) == revid) {
                          db.deleteRevision(revid)
                          db.updateRevisionParentId(revId2, parentId)
                          revisionsBlank += 1
                        }
                      }
                    } catch {
                      case e: Exception => {
                        database_logger.error(pageTitle + " -- " + revid + " -- " + e.getLocalizedMessage)
                        database_logger.error(e.getStackTraceString)
                      }
                    }
                  } else {
                    val wikiWriter = new FileWriter(revisionFile)
                    wikiWriter.write(wikitext)
                    wikiWriter.close()
                  }
                }

              }
              pagesDone += 1
              logger.info(pagesDone + "/" + pagesSize + "\t[" + ids.apply("ids").size + "/" + revision.getIds().apply("ids").size + " rev." + "]\t" + pageTitle)
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
    logger.info("Nb. total pages: " + pagesSize)
    logger.info("Nb. pages done: " + pagesDone)
    logger.info("Nb. revisions size: " + revisionsSize)
    logger.info("Nb. revisions done: " + revisionsDone)
    logger.info("Nb. new revisions: " + revisionsNew)
    logger.info("Nb. revisions suppressed: " + revisionsDel)
    logger.info("Nb. undo revisions: " + revisionsUndo)
    logger.info("Nb. blank revisions: " + revisionsBlank)
    logger.debug("Waiting for database threads to terminate...")
    while (db.isBusy()) {}
    val dbRevisions = db.browseRevisions()
    database_logger.info("Nb. pages: " + dbRevisions.groupBy(line => line.apply("title")).toList.size)
    database_logger.info("Nb. revisions: " + dbRevisions.size)
    logger.info("process finished.")
  }
}

