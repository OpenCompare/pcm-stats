package org.opencompare.stats.utils

import java.io.File

import com.almworks.sqlite4java._

import scala.collection.mutable.ListBuffer

/**
 * Created by smangin on 31/07/15.
 */
class DataBase(path : String) {

  // load the sqlite-JDBC driver using the current class loader
  private val connection = new SQLiteConnection(new File(path))
  connection.open(true)
  private val queue = new SQLiteQueue(new File(path))
  queue.start()

  val revisionModel = List(
    "id LONG PRIMARY KEY",
    "title TEXT",
    "date DATE",
    "lang TEXT",
    "author TEXT"
  )
  val metricModel =List(
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
    "changedCells INTEGER"
  )

  // create the schema
  def createTableRevisions() {
    connection.exec("drop table if exists revisions")
    connection.exec("create table revisions (" + revisionModel.mkString(", ") + ")")
  }
  def createTableMetrics() {
    connection.exec("drop table if exists metrics")
    connection.exec("create table metrics (" + metricModel.mkString(", ") + ")")
  }

  def getRevisions(): List[Map[String, Any]] = {
    val objects = ListBuffer[Map[String, Any]]()
    val job = new SQLiteJob[SQLiteStatement]() {
      protected def job(connection : SQLiteConnection): SQLiteStatement = {
        // this method is called from database thread and passes the connection
        connection.prepare("SELECT id, title, author, lang FROM revisions")
      }
    }
    // FIXME: Create a method that return a .complete() leads to several issues
    val result = queue.execute[SQLiteStatement, SQLiteJob[SQLiteStatement]](job).complete()
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

  def getMetrics(): List[Map[String, Any]] = {
    val objects = ListBuffer[Map[String, Any]]()
    val job = new SQLiteJob[SQLiteStatement]() {
      protected def job(connection : SQLiteConnection): SQLiteStatement = {
        // this method is called from database thread and passes the connection
        connection.prepare("SELECT id, name, compareToId FROM metrics")
      }
    }
    // FIXME: Create a method that return a .complete() leads to several issues
    val result = queue.execute[SQLiteStatement, SQLiteJob[SQLiteStatement]](job).complete()
    while (result.step()) {
      val element = Map(
        ("id", result.columnInt(0)),
        ("name", result.columnString(1)),
        ("compareToId", result.columnString(2))
      )
      objects.append(element)
    }
    objects.toList
  }

  def syncExecute(sql : String) {
    val job = new SQLiteJob[Unit]() {
      protected def job(connection : SQLiteConnection): Unit = {
        // this method is called from database thread and passes the connection
        try {
          connection.exec(sql)
        } catch {
          case e : SQLiteException => {
            println(sql)
            e.printStackTrace()
          }
        }
      }
    }
    // FIXME: Create a method that return a .complete() leads to several issues
    queue.execute[Unit, SQLiteJob[Unit]](job)
  }

  def hasThreadsLeft(): Boolean = {
    queue.isStopped
  }

}
