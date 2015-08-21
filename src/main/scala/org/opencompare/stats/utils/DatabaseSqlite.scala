package org.opencompare.stats.utils

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.almworks.sqlite4java._
import org.joda.time.DateTime
import org.opencompare.stats.interfaces.DatabaseInterface

import scala.collection.mutable.ListBuffer

/**
 * Created by smangin on 31/07/15.
 */
class DatabaseSqlite(path : String) extends DatabaseInterface {
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
    "id INTEGER",
    "parentId INTEGER REFERENCES revisions(id) ON UPDATE CASCADE",
    "title TEXT",
    "date DATETIME",
    "lang TEXT",
    "author TEXT",
    "CONSTRAINT pk PRIMARY KEY (id, parentId)"
  )
  private val metricModel = List(
    "id INTEGER REFERENCES revisions(id) ON UPDATE CASCADE",
    "name TEXT",
    "originalName TEXT",
    "date DATETIME",
    "compareToId INTEGER REFERENCES revisions(id) ON UPDATE CASCADE",
    "nbMatrices INTEGER",
    "changedMatrices INTEGER",
    "newFeatures INTEGER",
    "delFeatures INTEGER",
    "newProducts INTEGER",
    "delProducts INTEGER",
    "changedCells INTEGER"
    //"CONSTRAINT pk PRIMARY KEY (id, name, date, compareToId, nbMatrices)"

  )

  def initialize(): DatabaseSqlite = {
    val job = new SQLiteJob[Unit]() {
      protected def job(connection : SQLiteConnection): Unit = {
        // this method is called from database thread and passes the connection
        connection.exec("CREATE TABLE IF NOT EXISTS revisions (" + revisionModel.mkString(", ") + ")")
        connection.exec("DROP TABLE metrics")
        connection.exec("CREATE TABLE metrics (" + metricModel.mkString(", ") + ")")
      }
    }
    queue.execute[Unit, SQLiteJob[Unit]](job)
    this
  }

  def browseRevisions(): List[Map[String, Any]] = {
    val objects = ListBuffer[Map[String, Any]]()
    val job = new SQLiteJob[SQLiteStatement]() {
      protected def job(connection : SQLiteConnection): SQLiteStatement = {
        // this method is called from database thread and passes the connection
        connection.prepare("SELECT id, title, author, date, lang, parentId FROM revisions")
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
        ("parentId", result.columnInt(5))
      )
      objects.append(element)
    }
    objects.toList
  }

  def browseMetrics(): List[Map[String, Any]] = {
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

  def execute(sql : String) {
    val job = new SQLiteJob[Unit]() {
      protected def job(connection : SQLiteConnection): Unit = {
        // this method is called from database thread and passes the connection
        connection.exec(sql)
      }
    }
    // FIXME: Create a method that return a .complete() leads to several issues
    queue.execute[Unit, SQLiteJob[Unit]](job)
  }

  def isBusy(): Boolean = {
    queue.isStopped
  }

  def stopDB(): Unit = {
    queue.stop(true).join()
  }

  def revisionExists(id: Int): Boolean = {
    val job = new SQLiteJob[Boolean]() {
      protected def job(connection : SQLiteConnection): Boolean = {
        // this method is called from database thread and passes the connection
        val exists = connection.prepare("SELECT id FROM revisions WHERE id=" + id)
        exists.step()
        exists.hasRow
      }
    }
    queue.execute[Boolean, SQLiteJob[Boolean]](job).complete()
  }

  def createRevision(fields : Map[String, Any]): Option[Int] = {
    val id = fields.apply("id").asInstanceOf[Int]
    val title = fields.apply("title").asInstanceOf[String].replace("'", "")
    val date = fields.apply("date").asInstanceOf[DateTime].toString
    val author = fields.apply("author").asInstanceOf[String].replace("'", "")
    val parentId = fields.apply("parentId").asInstanceOf[Int]
    val lang = fields.apply("lang").asInstanceOf[String]

    val job = new SQLiteJob[Option[Int]]() {
      protected def job(connection : SQLiteConnection): Option[Int] = {
        // this method is called from database thread and passes the connection
        connection.prepare("INSERT INTO revisions VALUES (" +
          id + ", " +
          parentId + ", " +
          "'" + title + "', " +
          "'" + date + "', " +
          "'" + lang + "', " +
          "'" + author + "')"
        ).step
        Option(id)
      }
    }
    queue.execute[Option[Int], SQLiteJob[Option[Int]]](job).complete()
  }

  def createMetrics(fields : Map[String, Any]): Option[Int] = {
    val id = fields.apply("id").asInstanceOf[Int]
    val name = fields.apply("name").asInstanceOf[String].replace("'", "")
    val originalName = fields.apply("originalName").asInstanceOf[String].replace("'", "")
    val date = fields.apply("date").asInstanceOf[DateTime].toString
    val parentId = fields.apply("parentId").asInstanceOf[Int]
    val nbMatrices = fields.apply("nbMatrices").asInstanceOf[Int]
    val diffMatrices = fields.apply("diffMatrices").asInstanceOf[Int]
    val newFeatures = fields.apply("newFeatures").asInstanceOf[Int]
    val delFeatures = fields.apply("delFeatures").asInstanceOf[Int]
    val newProducts = fields.apply("newProducts").asInstanceOf[Int]
    val delProducts = fields.apply("delProducts").asInstanceOf[Int]
    val changedCells = fields.apply("changedCells").asInstanceOf[Int]

    val job = new SQLiteJob[Option[Int]]() {
      protected def job(connection : SQLiteConnection): Option[Int] = {
        // this method is called from database thread and passes the connection
        var result : Option[Int] = Option.empty
        //val exists = connection.prepare("SELECT id FROM metrics WHERE " +
        //    "id=" + id + " AND " +
        //    "originalName='" + originalName + "' AND " +
        //    "date='" + date + "' AND " +
        //    "compareToId=" + parentId + " AND " +
        //    "nbMatrices=" + nbMatrices)
        //exists.step()
        //if (!exists.hasRow) {
          connection.prepare("INSERT INTO metrics VALUES (" +
            id + ", " +
            "'" + name + "', " +
            "'" + originalName + "', " +
            "'" + date + "', " +
            parentId + ", " +
            nbMatrices + ", " +
            diffMatrices + ", " +
            newFeatures + ", " +
            delFeatures + ", " +
            newProducts + ", " +
            delProducts + ", " +
            changedCells + ")"
          ).step
          result = Option(id)
        //}
        result
      }
    }
    queue.execute[Option[Int], SQLiteJob[Option[Int]]](job).complete()
  }

  def deleteRevision(id : Int) {
    val job = new SQLiteJob[Unit]() {
      protected def job(connection : SQLiteConnection): Unit = {
        // this method is called from database thread and passes the connection
        connection.exec("DELETE FROM revisions WHERE id=" + id)
      }
    }
    queue.execute[Unit, SQLiteJob[Unit]](job).complete()
  }

  def updateRevisionParentId(id : Int, parentId : Int) {
    val job = new SQLiteJob[Unit]() {
      protected def job(connection : SQLiteConnection): Unit = {
        // this method is called from database thread and passes the connection
        connection.exec("UPDATE revisions SET parentId=" + parentId + " WHERE id=" + id)
      }
    }
    queue.execute[Unit, SQLiteJob[Unit]](job).complete()
  }

}
