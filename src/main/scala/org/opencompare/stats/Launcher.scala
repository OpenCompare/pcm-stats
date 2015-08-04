package org.opencompare.stats

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Logger, FileAppender}
import org.opencompare.io.wikipedia.io.MediaWikiAPI
import org.opencompare.stats.launchers.{Metrics, Revisions}
import org.opencompare.stats.utils.{CustomLoggerLayout, DataBase}

/**
 * Created by smangin on 03/08/15.
 */
object Launcher extends App {

  // Time
  val cTime = LocalDateTime.now()
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  // Paths
  val path = "metrics/"
  val wikitextPath = path + "wikitext/"
  // Logger
  val logger = Logger.getLogger("launcher")
  val fh = new FileAppender(new CustomLoggerLayout(), path + "metrics.log")
  logger.addAppender(fh)
  val revisions_logger = Logger.getLogger("revisions")
  revisions_logger.addAppender(fh)
  val metrics_logger = Logger.getLogger("metrics")
  metrics_logger.addAppender(fh)

  // Parser
  val api = synchronized(new MediaWikiAPI("https", "wikipedia.org"))
  // Database
  val db = new DataBase(path + "metrics.db")

  val revisions = new Revisions(api, db, cTime.format(formatter), wikitextPath, fh)
  val metrics = new Metrics(api, db, cTime.format(formatter), wikitextPath, fh)
  logger.info("Launcher starts the revision process...")
  revisions.start()
  logger.info("Launcher starts the metrics process...")
  metrics.start()
  logger.info("Launcher has stopped.")

}
