package org.opencompare.stats.utils

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.almworks.sqlite4java._
import org.opencompare.stats.interfaces.DatabaseInterface

import scala.collection.mutable.ListBuffer

/**
 * Created by smangin on 31/07/15.
 */
class DataBase(path : String) extends DatabaseInterface {

  // Time
  val cTime = LocalDateTime.now()
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  // load the sqlite-JDBC driver using the current class loader
  private val connection = new SQLiteConnection(new File(path))
  connection.open(true)
  connection.initializeBackup(new File(path + "_" + cTime.format(formatter) + ".db")).backupStep(-1)
  private val queue = new SQLiteQueue(new File(path))
  queue.start()

  private val revisionModel = List(
    "id LONG PRIMARY KEY",
    "parentId LONG REFERENCES revisions(id) ON UPDATE CASCADE",
    "title TEXT",
    "date DATETIME",
    "lang TEXT",
    "author TEXT"
  )
  private val metricModel = List(
    "id LONG REFERENCES revisions(id) ON UPDATE CASCADE",
    "name TEXT",
    "date DATETIME",
    "compareToId LONG REFERENCES revisions(id) ON UPDATE CASCADE",
    "nbMatrices INTEGER",
    "changedMatrices INTEGER",
    "newFeatures INTEGER",
    "delFeatures INTEGER",
    "newProducts INTEGER",
    "delProducts INTEGER",
    "changedCells INTEGER"
  )

  // create the schema
  def createTableRevisions() {
    val job = new SQLiteJob[Unit]() {
      protected def job(connection : SQLiteConnection): Unit = {
        // this method is called from database thread and passes the connection
        connection.exec("drop table if exists revisions")
        connection.exec("create table revisions (" + revisionModel.mkString(", ") + ")")
      }
    }
    queue.execute[Unit, SQLiteJob[Unit]](job)
  }
  def createTableMetrics() {
    val job = new SQLiteJob[Unit]() {
      protected def job(connection : SQLiteConnection): Unit = {
        // this method is called from database thread and passes the connection
        connection.exec("drop table if exists metrics")
        connection.exec("create table metrics (" + metricModel.mkString(", ") + ")")
      }
    }
    queue.execute[Unit, SQLiteJob[Unit]](job)
  }

  def getRevisions(): List[Map[String, Any]] = {
    val objects = ListBuffer[Map[String, Any]]()
    val job = new SQLiteJob[SQLiteStatement]() {
      protected def job(connection : SQLiteConnection): SQLiteStatement = {
        // this method is called from database thread and passes the connection
        connection.prepare("SELECT id, title, author, date, lang, parentid FROM revisions")
      }
    }
    // FIXME: Create a method that return a .complete() leads to several issues
    val result = queue.execute[SQLiteStatement, SQLiteJob[SQLiteStatement]](job).complete()
    while (result.step()) {
      val element = Map(
        ("id", result.columnInt(0)),
        ("title", result.columnString(1)),
        ("author", result.columnString(2)),
        ("date", result.columnString(3)),
        ("lang", result.columnString(4)),
        ("parentid", result.columnString(5))
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
        connection.exec(sql)
      }
    }
    // FIXME: Create a method that return a .complete() leads to several issues
    queue.execute[Unit, SQLiteJob[Unit]](job)
  }

  def hasThreadsLeft(): Boolean = {
    queue.isStopped
  }

  def revisionExists(id: Int): Boolean = {
    val job = new SQLiteJob[SQLiteStatement]() {
      protected def job(connection : SQLiteConnection): SQLiteStatement = {
        // this method is called from database thread and passes the connection
        connection.prepare(s"SELECT id FROM revisions where id=?").bind(1, id)
      }
    }
    val result = queue.execute[SQLiteStatement, SQLiteJob[SQLiteStatement]](job)
    result.complete().step()
  }

  def metricExists(id: Int, parentid: Int, title : String): Boolean = {
    val job = new SQLiteJob[SQLiteStatement]() {
      protected def job(connection : SQLiteConnection): SQLiteStatement = {
        // this method is called from database thread and passes the connection
        connection.prepare(s"SELECT id FROM revisions where id=?, parentid=?, title=?").bind(1, id).bind(2, parentid).bind(3, title)
      }
    }
    val result = queue.execute[SQLiteStatement, SQLiteJob[SQLiteStatement]](job)
    result.complete().step()
  }

}
