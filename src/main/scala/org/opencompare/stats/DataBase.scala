package org.opencompare.stats

import java.io.File

import com.almworks.sqlite4java._

/**
 * Created by blacknight on 31/07/15.
 */
class DataBase(path : String) {

  // load the sqlite-JDBC driver using the current class loader
  private val connection = new SQLiteConnection(new File(path))
  connection.open(true)
  private val queue = new SQLiteQueue(new File(path))
  queue.start()

  // create the schema
  connection.exec("drop table if exists revisions")
  connection.exec("drop table if exists metrics")
  connection.exec("create table revisions (" + List(
    "id LONG PRIMARY KEY",
    "title TEXT",
    "date DATE",
    "lang TEXT",
    "author TEXT"
  ).mkString(", ") + ")")
  connection.exec("create table metrics (" + List(
    "id LONG PRIMARY KEY",
    "name TEXT",
    "compareToId LONG REFERENCES revisions(id) ON UPDATE CASCADE",
    "nbMatrices INTEGER",
    "prevNbMatrices INTEGER",
    "changedMatrices INTEGER",
    "newFeatures INTEGER",
    "delFeatures INTEGER",
    "newProducts INTEGER",
    "delProducts INTEGER",
    "changedCells INTEGER"
  ).mkString(", ") + ")")

  def getRevisions(): List[Map[String, Any]] = {
    val job = new SQLiteJob[List[Map[String, Any]]]() {
      protected def job(connection : SQLiteConnection): List[Map[String, Any]] = {
        // this method is called from database thread and passed the connection
        val objects = List()
        val result = connection.prepare("SELECT id, title FROM revisions")
        while (result.step()) {
          objects ++ Map(
            ("id", result.columnInt(0)),
            ("title", result.columnString(1))
          )
        }
        objects
      }
    }
    queue.execute[List[Map[String, Any]], SQLiteJob[List[Map[String, Any]]]](job).complete()
  }

  def syncExecute(sql : String): Any = {
    val job = new SQLiteJob[Any]() {
      protected def job(connection : SQLiteConnection): Unit = {
        // this method is called from database thread and passed the connection
        try {
          connection.exec(sql)
        } catch {
          case _ : SQLiteException => {
            connection.prepare(sql)
          }
        }
      }
    }
    // FIXME: Create a method that return a .complete() leads to several issues
    queue.execute[Any, SQLiteJob[Any]](job)
  }

}
