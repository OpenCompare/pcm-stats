package org.opencompare.stats.launchers

import org.opencompare.io.wikipedia.io.MediaWikiAPI
import org.opencompare.stats.interfaces.RevisionsParserInterface
import play.api.libs.json.{JsNumber, JsObject, JsResultException, JsString}

import scala.collection.mutable.ListBuffer

/**
 * Created by smangin on 23/07/15.
 *
 * Used to get all revisions from a single wikipedia page by abstracting Xpath calls
 *
 */
class RevisionsParser (api : MediaWikiAPI, lang : String, title : String, direction : String = "newer") extends RevisionsParserInterface {

  var skipUndo = false
  var skipBlank = false
  private val revisions = api.getRevisionFromTitle(lang, title, direction)
  private var currentId = -1
  private val blankValues= List(
    "WP:AES",
    "WP:AUTOSUMM"
  )
  private val undoValues= List(
    "WP:UNDO",
    "WP:CLUEBOT",
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

  private def getIds(): List[Int] = {
    for (revision <- revisions) yield {
      (revision \ "revid").as[JsNumber].value.toIntExact
    }
  }

  def getIds(skipUndo : Boolean = false, skipBlank : Boolean = false): List[Int] = {
    val ids = getIds()
    val blackList = ListBuffer[Int]()
    ids.foreach(id => {
      if (skipUndo && isUndo(id)) {
        blackList.append(getParentId(id))
      }
      if (skipBlank && isBlank(id)) {
        blackList.append(id)
      }
    })
    ids - blankValues
  }

  def getDate(revid: Int): Option[String] = {
    val revision = getRevision(revid)
    if (revision.isDefined) {
      Some((revision.get \ "timestamp").as[JsString].value)
    } else {
      None
    }
  }

  def isBlank(revid: Int): Boolean = {
    var blank = false
    val revision = getRevision(revid)
    if (revision.isDefined) {
      try {
        val comment = (revision.get \ "comment")
        blankValues.foreach(value => {
          if (comment.as[JsString].value.contains(value)) {
            blank = true
          }
        })
      } catch {
        case e : JsResultException => false
      }
    }
    blank
  }

  def isUndo(revid: Int): Boolean = {
    var undo = false
    val revision = getRevision(revid)
    if (revision.isDefined) {
      try {
        val comment = (revision.get \ "comment")
        undoValues.foreach(value => {
          if (comment.as[JsString].value.contains(value)) {
            undo = true
          }
        })
      } catch {
        case e : JsResultException => false
      }
    }
    undo
  }

  def getParentId(revid: Int): Int = {
    var parentid = 0
    val revision = getRevision(revid)
    if (revision.isDefined) {
      parentid = (revision.get \ "parentid").as[JsNumber].value.toIntExact
    }
    parentid
  }

  def getAuthor(revid: Int): String = {
    val revision = getRevision(revid)
    if (revision.isDefined) {
      (revision.get \ "user").as[JsString].value
    }
    ""
  }

  def getWikitext(revid: Int): String = {
    api.getContentFromRevision(lang, revid)
  }

}
