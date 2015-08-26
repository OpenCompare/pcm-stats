package org.opencompare.stats

import java.io.{FileWriter, File}

import scala.io.Source
import scala.xml.pull._

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

}
