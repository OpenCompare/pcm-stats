package org.opencompare.stats

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.{FileAppender, Level, Logger}
import org.opencompare.io.wikipedia.io.MediaWikiAPI
import org.opencompare.stats.processes.{Revisions, Metrics}
import org.opencompare.stats.utils.{CustomLoggerLayout, DatabaseSqlite}

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
  val pcmPath = path + "pcm/"
  // Logger
  val level = Level.ALL
  val logger = Logger.getLogger("launcher")
  val fh = new FileAppender(new CustomLoggerLayout(), path + "metrics.log")
  logger.addAppender(fh)
  logger.setLevel(level)
  val revisions_logger = Logger.getLogger("revisions")
  revisions_logger.addAppender(fh)
  val metrics_logger = Logger.getLogger("metrics")
  metrics_logger.addAppender(fh)

  // Parser
  val api = synchronized(new MediaWikiAPI("https", "wikipedia.org"))
  // Database
  val db = new DatabaseSqlite(path + "metrics.db").initialize()

  val revisions = new Revisions(api, db, cTime.format(formatter), wikitextPath, fh, level)
  val metrics = new Metrics(api, db, cTime.format(formatter), wikitextPath, pcmPath, fh, level)
  revisions.compute(10)
  metrics.compute()
  logger.info("Launcher has stopped.")
}
