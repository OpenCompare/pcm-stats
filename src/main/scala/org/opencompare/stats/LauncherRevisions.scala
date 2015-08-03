package org.opencompare.stats

import java.io.{File, FileWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.github.tototoshi.csv.CSVReader
import org.apache.log4j.{FileAppender, Logger}
import org.opencompare.io.wikipedia.io.MediaWikiAPI
import org.opencompare.stats.utils.{CustomCsvFormat, CustomLoggerLayout, DataBase, RevisionsParser}

object LauncherRevisions extends App {

  // Time
  val cTime = LocalDateTime.now()
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  // File configurations
  val inputPageList = new File("src/main/resources/list_of_PCMs.csv")
  // Paths
  val path = "metrics/"
  val wikitextPath = path + "wikitext/"
  // Logger
  val fh = new FileAppender(new CustomLoggerLayout(), path + cTime.format(formatter) + "_process.log")
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
            try {
              // your custom behavior here
              val revision = new RevisionsParser(api, pageLang, pageTitle)
              revisionsSize += revision.getIds().size
              for (revid: Int <- revision.getIds()) {
                val sql = "insert into revisions values(" + revid + ", " + "\"" + pageTitle.replaceAll("\"", "") + "\", " + "\"" + revision.getDate(revid).get + "\", " + "\"" + pageLang + "\", " + "\"" + revision.getAuthor(revid).replaceAll("\"", "") + "\")"
                try {
                  db.syncExecute(sql)
                  // Save wikitext
                  val wikiWriter = new FileWriter(new File(wikitextPath + pageTitle + "-" + revid + ".wikitext"))
                  wikiWriter.write(revision.getWikitext(revid))
                  wikiWriter.close()
                  revisionsDone += 1
                } catch {
                  case e: Exception => {
                    println(sql)
                    e.printStackTrace()
                  }
                }
              }
              pagesDone += 1
              logger.info(pagesDone + "/" + pagesSize + "\t[" + revision.getIds().size + " rev." + "]\t" + pageTitle)
            } catch {
              case e: Exception => logger.fatal(pageTitle + " => " + e)
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
    logger.debug("Process => Nb. total pages: " + pagesSize)
    logger.debug("Process => Nb. pages done: " + pagesDone)
    logger.debug("Waiting for database threads to terminate...")
    while (db.hasThreadsLeft()) {}
    val done = db.getRevisions()
    logger.debug("Database => Nb. pages done: " + done.groupBy(line => line.apply("title")).toList.size)
    logger.debug("Database => Nb. revisions done: " + done.size)
  }
}

