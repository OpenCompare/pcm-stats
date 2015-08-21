package org.opencompare.stats

import java.io.{FileWriter, File}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.github.tototoshi.csv.CSVReader
import org.apache.log4j.{FileAppender, Level, Logger}
import org.opencompare.api.java.impl.io.KMFJSONExporter
import org.opencompare.io.wikipedia.io.{WikiTextLoader, MediaWikiAPI}
import org.opencompare.stats.processes.{Revisions, Metrics}
import org.opencompare.stats.utils.{WikiTextKeepTemplateProcessor, CustomCsvFormat, CustomLoggerLayout, DatabaseSqlite}

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.collection.JavaConversions._
import scala.io.Source

/**
 * Created by smangin on 03/08/15.
 */
object Launcher extends App {

  // Time
  val cTime = LocalDateTime.now()
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  // Paths
  val path = "metrics/"
  val outputPath = path + "code/"
  // Logger
  val level = Level.ALL
  val logger = Logger.getLogger("launcher")
  val fh = new FileAppender(new CustomLoggerLayout(), path + "metrics.log")
  logger.addAppender(fh)
  logger.setLevel(level)
  val revisions_logger = Logger.getLogger("revisions")
  revisions_logger.addAppender(fh)
  val mining_logger = Logger.getLogger("mining")
  mining_logger.addAppender(fh)
  val metrics_logger = Logger.getLogger("metrics")
  metrics_logger.addAppender(fh)

  val database_logger = Logger.getLogger("revisions.database")
  database_logger.addAppender(fh)

  // Parser
  val api = new MediaWikiAPI("https", "wikipedia.org")
  val wikiLoader = new WikiTextLoader(new WikiTextKeepTemplateProcessor(api))
  val kmfExporter = new KMFJSONExporter()

  // Database
  val db = new DatabaseSqlite(path + "metrics.db").initialize()

  val revisions = new Revisions(api, db, cTime.format(formatter), outputPath, fh, level)
  val metrics = new Metrics(api, db, cTime.format(formatter), outputPath, fh, level)

  val inputPageList = new File("src/main/resources/list_of_PCMs.csv")
  val reader = CSVReader.open(inputPageList)(new CustomCsvFormat)
  val pages = reader.allWithHeaders()
  reader.close()

  computeRevisionsAndMetrics()

  logger.info("Launcher has stopped.")

  def computeRevisionsAndMetrics(): Unit = {
    val revisionsResult = revisions.compute(pages)

    revisionsResult.onFailure {
      case e =>
        e.printStackTrace()
        revisions_logger.info("proccess failed")

        db.stopDB()
    }

    revisionsResult.onSuccess {
      case results => {

        val revisionsDone = results.map(_.doneRevs).sum
        val revisionsSize = results.map(_.sizeRevs).sum
        val revisionsNew = results.map(_.newRevs).sum
        val revisionsDel = results.map(_.delRevs).sum
        val revisionsUndo = results.map(_.undoRevs).sum
        val revisionsBlank = results.map(_.blankRevs).sum

        revisions_logger.info("Nb. total pages: " + pages.size)
        //        revisions_logger.info("Nb. pages done: " + pagesDone)
        revisions_logger.info("Nb. revisions size: " + revisionsSize)
        revisions_logger.info("Nb. revisions done: " + revisionsDone)
        revisions_logger.info("Nb. new revisions: " + revisionsNew)
        revisions_logger.info("Nb. revisions suppressed: " + revisionsDel)
        revisions_logger.info("Nb. undo revisions: " + revisionsUndo)
        revisions_logger.info("Nb. blank revisions: " + revisionsBlank)
        revisions_logger.debug("Waiting for database threads to terminate...")
        while (db.isBusy()) {}
        val dbRevisions = db.browseRevisions()
        database_logger.info("Nb. pages: " + dbRevisions.groupBy(line => line.apply("title")).toList.size)
        database_logger.info("Nb. revisions: " + dbRevisions.size)
        revisions_logger.info("process finished.")


        logger.info("mining PCMs")
        minePCMs()

        logger.info("computing metrics")
        computeMetrics()

      }
    }
  }

  def computeMetrics(): Unit = {
    val metricsResult = metrics.compute()

    metricsResult.onFailure {
      case _ =>
        metrics_logger.info("proccess failed")
        db.stopDB()
    }

    metricsResult.onSuccess {
      case results =>
        metrics_logger.info("All threads started, waiting to finish...")
        metrics_logger.info("Nb. revisions done (estimation): " + results.sum)
        metrics_logger.info("Waiting for database threads to terminate...")
        while (db.isBusy()) {}
        val done = db.browseMetrics()
        metrics_logger.info("Nb. comparisons done: " + done.size)
        metrics_logger.info("process finished.")

        db.stopDB()
    }
  }


  def minePCMs(): Unit = {

    db.browseRevisions().foreach(revision => {

      val pageId = revision("id").asInstanceOf[Int]
      val pageLang = revision("lang").asInstanceOf[String]
      val pageTitle = revision("title").asInstanceOf[String]

      val globalPath = outputPath + pageTitle.replace("'", "") + "/"
      val wikitextFilePath = globalPath + pageId + ".wikitext"
      val wikitext = Source.fromFile(wikitextFilePath).mkString
      val pcmContainers = wikiLoader.mine(pageLang, wikitext, pageTitle).toList

      for ((pcmContainer, index) <- pcmContainers.zipWithIndex) {
        val pcmFilePath = globalPath + pageId + "_" + index + ".json"
        if (!new File(pcmFilePath).exists()) {
          val kmfPCM = kmfExporter.export(pcmContainer)
          val kmfWriter = new FileWriter(new File(pcmFilePath))
          kmfWriter.write(kmfPCM)
          kmfWriter.close()
        }
      }
    })
  }
}

