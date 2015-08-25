package org.opencompare.stats

import java.io.File
import java.nio.file.{Paths, Files}

import scala.collection.mutable.ListBuffer
import scala.io.Source


import scala.collection.JavaConversions._
import scala.xml.XML

/**
 * Created by gbecan on 8/25/15.
 */
object WikipediaDumpProcessing extends App {


  val dumpFile = new File("wikipedia-dump/zuwiki-20150806-pages-articles-multistream.xml")
  val outputDir = new File(dumpFile.getAbsolutePath.substring(0, dumpFile.getAbsolutePath.size - ".xml".size))
  outputDir.mkdirs()



  // No parser solution
  var inPage = false
  var inRevision = false
  val extraContentBuilder = ListBuffer.empty[String]
  val pageBuilder = ListBuffer.empty[String]
  val revisionBuilder = ListBuffer.empty[String]

  for (line <- Source.fromFile(dumpFile).getLines()) {
    if (line.contains("<page>")) {

      inPage = true
      pageBuilder += line

    } else if (line.contains("</page>")) {

      pageBuilder += line
      inPage = false

      val page = pageBuilder.result()
      pageBuilder.clear()
      processPage(page)
    } else {

      if (inPage) {
        pageBuilder += line
      } else {
        extraContentBuilder += line
      }

    }
  }

  val extraContent = extraContentBuilder.result()

  val extraContentFile = new File(outputDir.getAbsolutePath + "/extraContent.xml")
  Files.write(extraContentFile.toPath, extraContent)


  def processPage(page : List[String]): Unit = {
    val pageXML = XML.loadString(page.mkString("\n"))
    val pageId = (pageXML \ "id").text
    val pageRevisionId = (pageXML \ "revision" \ "id").text

    val pageOutputDir = new File(outputDir.getAbsolutePath + "/" + pageId)
    pageOutputDir.mkdirs()

    val pageOutputFile = new File(pageOutputDir.getAbsolutePath + "/" + pageRevisionId + ".xml")

    Files.write(pageOutputFile.toPath, page)

  }

//  // Scales solution
//
//  val pullParser = pullXml(new FileInputStream(dumpFile))
//
//  while (pullParser.hasNext) {
//    pullParser.next() match {
//      case Left( i : XmlItem ) => //println(i)
//      // do something with an XmlItem
//      case Left( e : Elem ) if e.name.qName == "page" => println(e)
//      // do something with start of a new element
//      case Right(endElem) =>
//      // do something with the end of an element
//      case _ =>
//    }
//  }
//
//  println("processing file")
//  val it = iterate(List(UnprefixedQName("page", Namespace("http://www.mediawiki.org/xml/export-0.10/"))), pullParser)
//
//  for (elem <- it) {
//    println(elem.tree().fold())
//  }


//  // First solution
//
//  val xml = new XMLEventReader(Source.fromFile(dumpFile))
//
//  var insidePage = false
//  var buf = ListBuffer[String]()
//  for (event <- xml) {
//    event match {
//      case EvElemStart(_, "page", _, _) => {
//        insidePage = true
//        buf += backToXml(event)
//      }
//      case EvElemEnd(_, "page") => {
//        buf += backToXml(event)
//        insidePage = false
//
//        writePage(buf)
//        buf.clear
//      }
//      case e @ EvElemStart(_, tag, _, _) => {
//        println(e)
//        if (insidePage) {
//          buf += backToXml(event)
//        }
//      }
//      case e @ EvElemEnd(_, tag) => {
//        if (insidePage) {
//          buf += backToXml(event)
//        }
//      }
//      case EvText(t) => {
//        if (insidePage) {
//          buf += t
//        }
//      }
//      case _ => // ignore
//    }
//  }
//
//  def writePage(buf: ListBuffer[String]) = {
//    val s = buf.mkString
//    val x = XML.loadString(s)
//    val pageId = (x \ "id")(0).child(0).toString
//    val f = new File(outputDir, pageId + ".xml")
//    println("writing to: " + f.getAbsolutePath())
//    val out = new FileOutputStream(f)
//    out.write(s.getBytes())
//    out.close
//  }
//
//  def backToXml(ev : XMLEvent) = {
//    ev match {
//      case EvElemStart(pre, label, attrs, scope) => {
//        "<" + label + attrsToString(attrs) + ">"
//      }
//      case EvElemEnd(pre, label) => {
//        "</" + label + ">"
//      }
//      case _ => ""
//    }
//  }
//
//  def attrsToString(attrs : MetaData) = {
//    attrs.length match {
//      case 0 => ""
//      case _ => attrs.map( (m:MetaData) => " " + m.key + "='" + m.value +"'" ).reduceLeft(_+_)
//    }
//  }

}
