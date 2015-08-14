package org.opencompare.stats.interfaces

/**
 * Created by smangin on 04/08/15.
 *
 */
trait DatabaseInterface {

  // create the schema
  def initialize(): DatabaseInterface
  def browseRevisions(): List[Map[String, Any]]
  def browseMetrics(): List[Map[String, Any]]
  def execute(sql : String)
  def isBusy(): Boolean
  def deleteRevision(id : Int)
  def createMetrics(fields : Map[String, Any]): Option[Int]
  def createRevision(fields : Map[String, Any]): Option[Int]

}
