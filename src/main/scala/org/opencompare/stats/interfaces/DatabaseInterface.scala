package org.opencompare.stats.interfaces

/**
 * Created by smangin on 31/07/15.
 */
trait DatabaseInterface {

  // create the schema
  def createTableRevisions()
  def createTableMetrics()
  def getRevisions(): List[Map[String, Any]]
  def getMetrics(): List[Map[String, Any]]
  def syncExecute(sql : String)
  def hasThreadsLeft(): Boolean
  def revisionExists(id: Int): Boolean

}
