package org.opencompare.stats

import java.io.File

import com.almworks.sqlite4java._

import scala.collection.mutable.ListBuffer

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
  def createTableRevisions() {
    connection.exec("drop table if exists revisions")
    connection.exec("create table revisions (" + List(
      "id LONG PRIMARY KEY",
      "title TEXT",
      "date DATE",
      "lang TEXT",
      "author TEXT"
    ).mkString(", ") + ")")
  }
  def createTableMetrics() {
    connection.exec("drop table if exists metrics")
    connection.exec("create table metrics (" + List(
      "id LONG REFERENCES revisions(id) ON UPDATE CASCADE",
      "name TEXT",
      "compareToId LONG REFERENCES revisions(id) ON UPDATE CASCADE",
      "nbMatrices INTEGER",
      "prevNbMatrices INTEGER",
      "changedMatrices INTEGER",
      "newFeatures INTEGER",
      "delFeatures INTEGER",
      "newProducts INTEGER",
      "delProducts INTEGER",
      "changedCells INTEGER",
      "CONSTRAINT pk PRIMARY KEY (id, name, compareToId)"
    ).mkString(", ") + ")")
  }

  def getRevisions(): List[Map[String, Any]] = {
    val objects = ListBuffer[Map[String, Any]]()
    val result = connection.prepare("SELECT id, title, author, lang FROM revisions")
    while (result.step()) {
      val element = Map(
        ("id", result.columnInt(0)),
        ("title", result.columnString(1)),
        ("author", result.columnString(2)),
        ("lang", result.columnString(3))
      )
      objects.append(element)
    }
    objects.toList
  }

  def syncExecute(sql : String): Any = {
    val job = new SQLiteJob[Any]() {
      protected def job(connection : SQLiteConnection): Unit = {
        // this method is called from database thread and passed the connection
        try {
          connection.exec(sql)
        } catch {
          case e : SQLiteException => {
            e.printStackTrace()
            println(sql)
            connection.prepare(sql)
          }
        }
      }
    }
    // FIXME: Create a method that return a .complete() leads to several issues
    queue.execute[Any, SQLiteJob[Any]](job)
  }

}
