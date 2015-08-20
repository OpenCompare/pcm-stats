package org.opencompare.stats.processes

import java.io.{File, FileWriter}

import com.github.tototoshi.csv.CSVReader
import org.apache.log4j.{FileAppender, Level, Logger}
import org.joda.time.DateTime
import org.opencompare.io.wikipedia.io.MediaWikiAPI
import org.opencompare.stats.utils.{CustomCsvFormat, DatabaseSqlite, RevisionsParser}

import scala.concurrent._
import ExecutionContext.Implicits.global

/**
 * Created by smangin on 23/07/15.
 */
class Revisions(api : MediaWikiAPI, db : DatabaseSqlite, time : String, wikitextPath : String, appender : FileAppender, level : Level) {


  // Logging
  private val logger = Logger.getLogger("revisions")
  logger.addAppender(appender)
  logger.setLevel(level)


  def compute(pages : List[Map[String, String]]) : Future[List[PageStats]] = {
    logger.info("Started...")


    val tasks = for (page <- pages) yield {
      Future {

        // Statistical vars
        var revisionsDone = 0
        var revisionsSize = 0
        var revisionsUndo = 0
        var revisionsBlank = 0
        var revisionsNew = 0
        var revisionsDel = 0



        val pageLangOption = page.get("Lang")
        val pageTitleOption = page.get("Title")

        if (pageLangOption.isDefined && pageTitleOption.isDefined) {
          val pageLang = pageLangOption.get
          val pageTitle = pageTitleOption.get

          val file = wikitextPath + pageTitle.replace("'", "") + "/"
          new File(file).mkdirs()
          try {

            val revision = new RevisionsParser(api, pageLang, pageTitle, "older")
            val ids = revision.getIds(true, true)

            revisionsSize += ids("ids").size
            revisionsUndo += ids("undo").size
            revisionsBlank += ids("blank").size
            revisionsDel += ids("suppressed").size


            for (revid: Int <- ids("ids")) {

              val fileName = file + revid + ".wikitext"
              val revisionFile = new File(fileName)
              val parentId = revision.getParentId(revid)
              // Keep an eye on already existing revisions
              if (!db.revisionExists(revid)) {
                db.createRevision(Map(
                  ("id", revid),
                  ("title", pageTitle),
                  ("date", DateTime.parse(revision.getDate(revid).get)),
                  ("author", revision.getAuthor(revid)),
                  ("parentId", parentId),
                  ("lang", pageLang)
                ))
                revisionsNew += 1
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
                  case _ : Exception => println("error")
                }
                // Manage undo empty revisions which has not been indicated by the revision parser(based on wikipedia metadata)
                if (wikitext == "") {
                  // We change the parent revision of the parent revision with the revision which is the parent revision of the current revision... Gniark gniark
                  var revidToCompareWith = 0
                  for (revId2 <- revision.getIds()("ids")) {
                    if (revision.getParentId(revId2) == revid) {
                      db.deleteRevision(revid)
                      db.updateRevisionParentId(revId2, parentId)
                      revisionsBlank += 1
                      println("blank")
                    }
                  }
                } else {
                  val wikiWriter = new FileWriter(revisionFile)
                  wikiWriter.write(wikitext)
                  wikiWriter.close()
                }
              }

            }
          } catch {
            case e: Exception => {
              logger.error(pageTitle + " => " + e.getLocalizedMessage)
              logger.error(e.getStackTrace)
            }
          }

          PageStats(revisionsDone, revisionsSize, revisionsUndo, revisionsBlank, revisionsNew, revisionsDel)
        } else {
          PageStats(0, 0, 0, 0, 0, 0)
        }
      }
    }

    val aggregatedTasks = Future.sequence(tasks)

    aggregatedTasks
  }
}


