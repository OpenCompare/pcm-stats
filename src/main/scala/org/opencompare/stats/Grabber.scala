package org.opencompare.stats

import java.text.DateFormat
import java.util.{Date, Locale}

import org.opencompare.io.wikipedia.io.MediaWikiAPI
import play.api.libs.json.{JsArray, JsValue}

/**
 * Created by smangin on 7/23/15.
 *
 * Used to get all revisions from a single wikipedia page
 *
 */
class Grabber (lang : String, title : String) {


  private val api: MediaWikiAPI = new MediaWikiAPI("https", "wikipedia.org")
  val json = api.getRevisionFromTitle(lang, title)

  def getRevIds(): List[String] = {
    var revids: List[String] = List[String]()
    for (value: JsValue <- json) {
      var revid = value \ "revid"
      revids :+ revid.toString
    }
    revids
  }

  def getDate(revid: String): Date = {
    var timestamp = ""
    for (value: JsValue <- json) {
      if ((value \ "revid").equals(revid)) {
        timestamp = (value \ "timestamp").toString
      }
    }
    if (timestamp != "") {
      val df = DateFormat.getDateInstance(DateFormat.LONG, Locale.FRANCE);
      df.parse(timestamp)
    } else {
      new Date()
    }
  }

  def getAuthor(revid: String): String = {
    var author = ""
    for (value: JsValue <- json) {
      if ((value \ "revid").equals(revid)) {
        author = (value \ "user").toString
      }
    }
    author
  }
}
