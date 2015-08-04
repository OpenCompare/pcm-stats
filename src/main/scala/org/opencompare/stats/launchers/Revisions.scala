package org.opencompare.stats.launchers

import java.io.{File, FileWriter}

import com.github.tototoshi.csv.CSVReader
import org.apache.log4j.{FileAppender, Logger}
import org.opencompare.io.wikipedia.io.MediaWikiAPI
import org.opencompare.stats.utils.{CustomCsvFormat, DataBase, RevisionsParser}

class Revisions(api : MediaWikiAPI, db : DataBase, time : String, wikitextPath : String, appender : FileAppender) {

  // File configurations
  private val inputPageList = new File("src/main/resources/list_of_PCMs.csv")
  private val logger = Logger.getLogger("revisions")
  private val database_logger = Logger.getLogger("revisions.database")
  logger.addAppender(appender)
  database_logger.addAppender(appender)

  def start() {
    if (!new File(wikitextPath).exists()) {
      db.createTableRevisions()
    }
    // Parse wikipedia page list
    val reader = CSVReader.open(inputPageList)(new CustomCsvFormat)
    val pages = reader.allWithHeaders()
    val groups = pages.grouped(10).toList // Performance issue hack
    var pagesSize = pages.size
    var pagesDone = synchronized(0)
    var revisionsSize = synchronized(0)
    var revisionsDone = synchronized(0)
    val groupThread = new ThreadGroup("revisions")
    groups.foreach(group => {
      var pageTitle = group.head.get("Title").get
      val thread = new Thread(groupThread, pageTitle) {
        override def run() {
          group.foreach(line => {
            val pageLang = line.get("Lang").get
            pageTitle = line.get("Title").get
            logger.debug(pageTitle + " started...")
            val file = wikitextPath + pageTitle + "/"
            new File(file).mkdirs()
            try {
              // your custom behavior here
              val revision = new RevisionsParser(api, pageLang, pageTitle, "older")
              revisionsSize += revision.getIds().size
              for (revid: Int <- revision.getIds()) {
                // Keep an eye on already existing revisions
                if (db.revisionExists(revid)) {
                  logger.info(pageTitle + " already done !")
                } else {
                  // To prevent from matrix deletion then addition,  delete the parentid from the database (it should be here because of the older to newer sorting)
                  if (revision.isUndo(revid)) {
                    val parentId = revision.getParentId(revid)
                    logger.debug(pageTitle + " => '" + revid + "' is an undo revision of '" + parentId + "'")
                    val sql = "delete from revisions where id=" + parentId
                    db.syncExecute(sql)
                  }
                  val sql = "insert into revisions values(" + revid + ", " + "\"" + pageTitle.replaceAll("\"", "") + "\", " + "\"" + revision.getDate(revid).get + "\", " + "\"" + pageLang + "\", " + "\"" + revision.getAuthor(revid).replaceAll("\"", "") + "\")"
                  val fileName = file + revid + ".wikitext"
                  try {
                    db.syncExecute(sql)
                    // Save wikitext
                    val wikiWriter = new FileWriter(new File(fileName))
                    wikiWriter.write(revision.getWikitext(revid))
                    wikiWriter.close()
                    revisionsDone += 1
                  } catch {
                    case e: Exception => {
                      database_logger.error(pageTitle + " => " + e.getLocalizedMessage)
                      database_logger.error("SQL command => " + sql)
                      database_logger.error("Wikitext filename => " + fileName)
                      database_logger.error(e.getStackTraceString)
                    }
                  }
                }
              }
              pagesDone += 1
              logger.info(pagesDone + "/" + pagesSize + "\t[" + revision.getIds().size + " rev." + "]\t" + pageTitle)
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
    logger.info("Process => Nb. total pages: " + pagesSize)
    logger.info("Process => Nb. pages done: " + pagesDone)
    logger.debug("Waiting for database threads to terminate...")
    while (db.hasThreadsLeft()) {}
    val done = db.getRevisions()
    logger.info("Database => Nb. pages done: " + done.groupBy(line => line.apply("title")).toList.size)
    logger.info("Database => Nb. revisions done: " + done.size)
    logger.info("process finished.")
  }
}

