package org.opencompare.stats.interfaces

/**
 * Created by smangin on 04/08/15.
 *
 */
trait RevisionsParserInterface {

  def getIds(skipUndo : Boolean = false, skipBlank : Boolean = false): Map[String, List[Int]]
  def getDate(revid: Int): Option[String]
  def isUndo(revid: Int): Boolean
  def getParentId(revid: Int): Int
  def getAuthor(revid: Int): String
  def getWikitext(revid: Int): String
}
