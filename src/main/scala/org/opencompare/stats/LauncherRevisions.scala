package org.opencompare.stats

import java.io.{File, FileWriter}
import java.sql.SQLException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.github.tototoshi.csv.CSVReader
import org.apache.log4j.{FileAppender, Logger, SimpleLayout}
import org.opencompare.io.wikipedia.io.MediaWikiAPI

object LauncherRevisions extends App {
  // Time
  val cTime = LocalDateTime.now()
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val fullFormatter = DateTimeFormatter.ofPattern("HH:mm:ss dd/MM/yyyy")
  // File configurations
  val inputPageList = new File("src/main/resources/list_of_PCMs.csv")
  // Paths
  val path = "revisions/"
  val wikitextPath = path + "wikitext/"
  // Logger
  val fh = new FileAppender(new SimpleLayout(), path + cTime.format(formatter) + "_process.log")
  // Parser
  val api: MediaWikiAPI = synchronized(new MediaWikiAPI("https", "wikipedia.org"))
  // Database
  val db = new DataBase(path + cTime.format(formatter) +".db")

  if (!new File(wikitextPath).exists()) {
    db.createTableRevisions()
    // Creates directories with leaf paths
    new File(wikitextPath).mkdirs()

    val logger = Logger.getLogger("revisions")
    logger.addAppender(fh)
    logger.debug("Started at " + LocalDateTime.now().format(fullFormatter))

    // Parse wikipedia page list
    val reader = CSVReader.open(inputPageList)(new CustomCsvFormat)
    var done = synchronized(1)
    val pages = reader.allWithHeaders()
    val groups = pages.grouped(10).toList // Performance issue hack
    var pagesSize = pages.size
    var revisionsSize = 0
    var revisionsDone = 0
    logger.debug("Nb. pages: " + pages.size)
    val groupThread = new ThreadGroup("revisions")
    groups.foreach(group => {
      var pageTitle = group.head.get("Title").get
      val thread = new Thread(groupThread, pageTitle) {
        override def run() {
          group.foreach(line => {
            val pageLang = line.get("Lang").get
            pageTitle = line.get("Title").get
            try {
              // your custom behavior here
              val revision = new Revision(api, pageLang, pageTitle)
              var revisionsSize = revision.getIds().size
              for (revid: Int <- revision.getIds()) {
                revisionsSize += 1
                val sql = "insert into revisions values(" + revid + ", " + "\"" + revision.getTitle.replaceAll("\"", "") + "\", " + "\"" + revision.getDate(revid).get + "\", " + "\"" + revision.getLang + "\", " + "\"" + revision.getAuthor(revid).replaceAll("\"", "") + "\")"
                try {
                  db.syncExecute(sql)
                  revisionsDone += 1
                } catch {
                  case e: SQLException => {
                    e.printStackTrace()
                    println(sql)
                  }
                }
                // Save wikitext
                val wikiWriter = new FileWriter(new File(wikitextPath + revision.getTitle + "-" + revid + ".wikitext"))
                wikiWriter.write(revision.getWikitext(revid))
                wikiWriter.close()
              }
              logger.info(done + "/" + pagesSize + "\t[" + revisionsSize + " rev." + "]\t" + pageTitle)
              done += 1
            } catch {
              case e: Throwable => {
                logger.fatal(pageTitle + " => " + e)
              }
            }
          })
        }
      }
      thread.join()
      thread.start()
      Thread.sleep(100) // slow the loop down a bit
    })
    while (groupThread.activeCount() > 0) {}
    logger.debug("Nb. total revisions: " + revisionsSize)
    logger.debug("Nb. revisions done: " + revisionsDone)
    logger.debug("Ended at " + LocalDateTime.now().format(fullFormatter))
  }
}

