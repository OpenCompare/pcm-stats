package org.opencompare.stats

import java.io._

import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream

import scala.io.Source
import scala.xml.pull._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

/**
 * Created by gbecan on 8/25/15.
 */
class WikipediaDumpProcessor {

  def cutDump(dumpFile : File, outputDir : File): Unit = {
    outputDir.mkdirs()

    val sourceFile = Source.fromFile(dumpFile)
    val xmlReader = new XMLEventReader(sourceFile)

    val builder = StringBuilder.newBuilder

    var inPage = false
    var inRevision = false
    var inId = false
    var revisionIdDone = false

    var pageId = ""
    var revisionId = ""

    for (event <- xmlReader) {
      event match {
        case EvElemStart(pre, label, attrs, scope) =>
          label match {
            case "page" => inPage = true
            case "revision" =>
              inRevision = true
              revisionIdDone = false
            case "id" if inPage => inId = true
            case _ =>
          }

          val attributeString = if (attrs.isEmpty) {
            ""
          } else {
            val attrString = for (attr <- attrs) yield {
              val values = for (value <- attr.value) yield {
                attr.prefixedKey + "=\"" + value.mkString + "\""
              }
              values.mkString(" ", " ", "")
            }
            attrString.mkString(" ")
          }

          if (inRevision) {
            builder ++= "<" + label + attributeString + ">"
          }

        case EvElemEnd(pre, label) =>
          if (inRevision) {
            builder ++= "</" + label + ">"
          }

          label match {
            case "page" => inPage = false
            case "revision" =>
              inRevision = false
              val content = builder.result()
              builder.clear()

              val pageOutputDir = new File(outputDir.getAbsolutePath + "/" + pageId)
              pageOutputDir.mkdirs()
              val revisionOutputFile = new File(pageOutputDir.getAbsolutePath + "/" + revisionId + ".xml")

              val writer = new FileWriter(revisionOutputFile)
              writer.write(content)
              writer.close()

            case "id" if inRevision =>
              inId = false
              revisionIdDone = true
            case "id" if inPage => inId = false
            case _ =>
          }

        case EvEntityRef(entity) =>
          if (inRevision) {
            builder ++= "&" + entity + ";"
          }

        case EvProcInstr(target, text) =>

        case EvText(text) =>
          if (inId && inPage && !inRevision) {
            pageId = text
          } else if (inId && inRevision && !revisionIdDone) {
            revisionId = text
          }

          if (inRevision) {
            builder ++= text
          }
        case EvComment(text) =>
      }
    }
  }

  def countTablesInCompressedDump(compressedDumpFile : File) : Int = {

    var nbOfTables = 0
    var nbOfRevs = 0
    var nbOfRevsWithTables = 0

    // Create compressed input stream
    val fin = new FileInputStream(compressedDumpFile)
    val in = new BufferedInputStream(fin)
    val bzIn = new BZip2CompressorInputStream(in, true)

    // Create XML reader
    val sourceFile = Source.fromInputStream(bzIn)
    val xmlReader = new XMLEventReader(sourceFile)

    var inRevision = false
    var inRevisionContent = false
    var inRevId = false
    var doneRevId = false

    var revContainsTable = false

    var revId = ""

    for (event <- xmlReader) {
      event match {
        case EvElemStart(pre, label, attrs, scope) =>
          label match {
            case "revision" =>
              inRevision = true
              doneRevId = false
            case "text" if inRevision => inRevisionContent = true
            case "id" if inRevision => inRevId = true
            case _ =>
          }
        case EvElemEnd(pre, label) =>
          label match {
            case "revision" =>
              nbOfRevs += 1
              inRevision = false
              if (revContainsTable) {
                nbOfRevsWithTables += 1

                if (nbOfRevsWithTables % 1000 == 0) {
                  println(nbOfRevsWithTables + " / " + nbOfRevs)
                }

              }
              revContainsTable =false
            case "text" if inRevisionContent => inRevisionContent = false
            case "id" if inRevId => doneRevId = true
            case _ =>
          }

        case EvEntityRef(entity) =>
        case EvProcInstr(target, text) =>
        case EvText(text) if inRevisionContent =>
          if (text.contains("{|")) {
            revContainsTable = true
//            println(revId + " : table detected")
//            if (nbOfTables % 1000 == 0) {
//              println(nbOfTables)
//            }
            nbOfTables += 1
          }
        case EvText(text) if inRevId && !doneRevId => revId = text
        case EvText(text) =>
        case EvComment(text) =>
      }
    }

    bzIn.close()

    println("#revs = " + nbOfRevs)
    println("#revs with tables = " + nbOfRevsWithTables)
    println("#tables = " + nbOfTables)

    nbOfTables
  }

}
