package org.opencompare.stats

import java.io.{FileWriter, File}

import org.opencompare.io.wikipedia.io.{MediaWikiAPI, WikiTextLoader}
import org.opencompare.stats.utils.WikiTextKeepTemplateProcessor
import org.xml.sax.SAXParseException

import scala.io.Source
import scala.xml.{XML, MetaData, NamespaceBinding}
import scala.xml.pull._

/**
 * Created by gbecan on 8/25/15.
 */
object WikipediaDumpProcessing extends App {


  val dumpFile = new File("wikipedia-dump/zuwiki-20150806-pages-articles-multistream.xml")
//  val dumpFile = new File("wikipedia-dump/zuwiki-20150806-pages-meta-history.xml")

  val outputDir = new File(dumpFile.getAbsolutePath.substring(0, dumpFile.getAbsolutePath.size - ".xml".size))

  // Cut dump in pageId/revisionId.xml files
//  val processor = new WikipediaDumpProcessor
//  processor.cutDump(dumpFile, outputDir)

  // Read extracted XML files, detect tables and extract PCMs

  val api = new MediaWikiAPI("wikipedia.org")
  val pcmMiner = new WikiTextLoader(new WikiTextKeepTemplateProcessor(api))

  for (pageDir <- outputDir.listFiles() if pageDir.isDirectory) {
    for (revisionFile <- pageDir.listFiles() if revisionFile.isFile && revisionFile.getName.endsWith(".xml")) {
      val revXml = XML.loadFile(revisionFile)
      val revId = (revXml \ "id").text
      val revContent = (revXml \ "text").text
      val containsTable = revContent.contains("{|")

      if (containsTable) {
        val pcmContainers = pcmMiner.load(revContent)
        println(pcmContainers.size())
      }

    }
  }
}
