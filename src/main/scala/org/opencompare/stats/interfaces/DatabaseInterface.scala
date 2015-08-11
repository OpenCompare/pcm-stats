package org.opencompare.stats.interfaces

/**
 * Created by smangin on 04/08/15.
 *
 */
trait DatabaseInterface {

  // create the schema
  def initialize(): DatabaseInterface
  def getRevisions(): List[Map[String, Any]]
  def getMetrics(): List[Map[String, Any]]
  def execute(sql : String)
  def isBusy(): Boolean
  def deleteRevision(id : Int)
  def insertMetrics(fields : Map[String, Any]): Boolean
  def insertRevision(fields : Map[String, Any]): Boolean

}
