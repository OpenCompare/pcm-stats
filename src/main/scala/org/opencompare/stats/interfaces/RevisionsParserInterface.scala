package org.opencompare.stats.interfaces

/**
 * Created by smangin on 7/23/15.
 *
 * Used to get all revisions from a single wikipedia page by abstracting Xpath calls
 *
 */
trait RevisionsParserInterface {

  def getIds(): List[Int]
  def getDate(revid: Int): Option[String]
  def isUndo(revid: Int): Boolean
  def getParentId(revid: Int): Int
  def getAuthor(revid: Int): String
  def getWikitext(revid: Int): String
}
