package org.opencompare.stats.utils

import org.opencompare.io.wikipedia.io.MediaWikiAPI
import play.api.libs.json.{JsNumber, JsObject, JsString}

/**
 * Created by smangin on 7/23/15.
 *
 * Used to get all revisions from a single wikipedia page by abstracting Xpath calls
 *
 */
class RevisionsParser (api : MediaWikiAPI, lang : String, title : String, direction : String = "newer") {


  private val revisions = api.getRevisionFromTitle(lang, title, direction)
  private var currentId = -1
  private val undoValues= List(
    "WP:UNDO",
    "WP:\"",
    "WP:REVERT",
    "WP:REV",
    "WP:RV"
  )

  private def getRevision(id: Int): Option[JsObject] = {
    revisions.find( revision => id == getId(revision))
  }

  private def getId(revision: JsObject): Int = {
    (revision \ "revid").as[JsNumber].value.toIntExact
  }

  def getIds(): List[Int] = {
    for (revision <- revisions) yield {
      (revision \ "revid").as[JsNumber].value.toIntExact
    }
  }

  def getDate(revid: Int): Option[String] = {
    val revision = getRevision(revid)
    if (revision.isDefined) {
      Some((revision.get \ "timestamp").as[JsString].value)
    } else {
      None
    }
  }

  def isUndo(revid: Int): Boolean = {
    val revision = getRevision(revid)
    if (revision.isDefined) {
      undoValues.foreach(value => {
        if ((revision.get \ "comment").as[JsString].value.contains(value)) {
          true
        }
      })
    }
    false
  }

  def getParentId(revid: Int): Int = {
    val revision = getRevision(revid)
    if (revision.isDefined) {
      (revision.get \ "parentid").as[JsNumber].value.toIntExact
    }
    0
  }

  def getAuthor(revid: Int): String = {
    val revision = getRevision(revid)
    if (revision.isDefined) {
      (revision.get \ "user").as[JsString].value
    }
    ""
  }

  def getWikitext(revid: Int): String = {
    revisions.foreach(revision => {
      if (revid == getId(revision) && "wikitext" == (revision \ "contentmodel").as[JsString].value) {
        (revision \ "*").as[JsString].value
      }
    })
    ""
  }

}
