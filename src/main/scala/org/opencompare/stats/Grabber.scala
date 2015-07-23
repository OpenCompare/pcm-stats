package org.opencompare.stats

import java.text.DateFormat
import java.util
import java.util.{Date, Locale}

/**
 * Created by smangin on 7/23/15.
 *
 * Use to get all revision from a single wikipedia page
 *
 */
class Grabber (lang : String, title : String) {


  private val api: MediaWikiAPI = new MediaWikiAPI("https", "wikipedia.org")
  val json = api.getRevisionFromTitle(lang, title)

  def getRevIds(): util.List[String] = {
    val revids: util.List[String] = new util.ArrayList();
    for (value: JsValue <- json) {
      var revid = value \ "revid"
      revids.add(revid.toString)
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
