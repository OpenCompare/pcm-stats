package org.opencompare.stats

import org.opencompare.io.wikipedia.io.MediaWikiAPI
import play.api.libs.json.{JsNumber, JsObject, JsString}

/**
 * Created by smangin on 7/23/15.
 *
 * Used to get all revisions from a single wikipedia page
 *
 */
class Revision (lang : String, title : String) {


  private val api: MediaWikiAPI = new MediaWikiAPI("https", "wikipedia.org")
  private val revisions = api.getRevisionFromTitle(lang, title, 50)

  def getId(revision: JsObject): Int = {
    (revision \ "revid").as[JsNumber].value.toIntExact
  }

  def getIds(): List[Int] = {
    for (revision <- revisions) yield {
      (revision \ "revid").as[JsNumber].value.toIntExact
    }
  }

  def getDate(revid: Int): Option[String] = {
    val revision = revisions.find( revision => revid.equals(getId(revision)))
    if (revision.isDefined) {
      Some((revision.get \ "timestamp").as[JsString].value)
    } else {
      None
    }
  }

  def getAuthor(revid: Int): String = {
    var author = ""
    for (revision <- revisions) {
      if (revid == getId(revision)) {
        author = (revision \ "user").as[JsString].value
      }
    }
    author
  }

  def getWikitext(revid: Int): String = {
    var content = ""
    for (revision <- revisions) {
      if (revid == getId(revision) && "wikitext" == (revision \ "contentmodel").as[JsString].value) {
        content = (revision \ "*").as[JsString].value
      }
    }
    content
  }

}
