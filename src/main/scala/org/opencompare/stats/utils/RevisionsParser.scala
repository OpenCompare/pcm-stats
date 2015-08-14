package org.opencompare.stats.utils

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
  private val ids = for (revision <- revisions) yield {
    (revision \ "revid").as[JsNumber].value.toIntExact
  }

  private var currentId = -1
  private val blankValues= List(
    "WP:AES",
    "WP:BLANK",
    "WP:PAGEBLANKING",
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

  def getIds(skipUndo : Boolean = false, skipBlank : Boolean = false): Map[String, List[Int]] = {
    val undoBlackList = ListBuffer[Int]()
    val blankBlackList = ListBuffer[Int]()
    ids.foreach(id => {
      if (skipUndo && isUndo(id)) {
        undoBlackList.append(getParentId(id))
      }
      if (skipBlank && isBlank(id)) {
        blankBlackList.append(id)
      }
    })
    Map[String, List[Int]](
      ("ids", ids.diff(undoBlackList ++ blankBlackList).toList),
      ("undo", undoBlackList.toList),
      ("blank", blankBlackList.toList)
    )
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
    var parentId = 0
    val revision = getRevision(revid)
    if (revision.isDefined) {
      parentId = (revision.get \ "parentid").as[JsNumber].value.toIntExact
      if (isUndo(revid)) {
        parentId = getParentId(ids.find(id => (id == parentId)).get)
      }
      if (isBlank(parentId)) {
        parentId = getParentId(parentId)
      }
    }
    parentId
  }

  def getAuthor(revid: Int): String = {
    var author = ""
    val revision = getRevision(revid)
    if (revision.isDefined) {
      author = (revision.get \ "user").as[JsString].value
    }
    author
  }

  def getWikitext(revid: Int): String = {
    api.getContentFromRevision(lang, revid)
  }

}
