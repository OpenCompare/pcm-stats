package org.opencompare.stats

import java.sql.DriverManager

/**
 * Created by blacknight on 31/07/15.
 */
class DataBase(path : String) {

  Class.forName("org.sqlite.JDBC")
  // load the sqlite-JDBC driver using the current class loader
  val connection = DriverManager.getConnection("jdbc:sqlite:" + path)
  connection.setAutoCommit(true)

  // create the schema
  val statement = connection.createStatement()
  statement.execute("drop table if exists revisions")
  statement.execute("drop table if exists metrics")
  statement.execute("create table revisions (" + List(
    "id LONG PRIMARY KEY",
    "title TEXT",
    "date DATE",
    "lang TEXT",
    "author TEXT"
  ).mkString(", ") + ")")
  statement.execute("create table metrics (" + List(
    "id LONG PRIMARY KEY",
    "pageId LONG REFERENCES metrics(id) ON UPDATE CASCADE",
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
    val objects = List()
    val result = statement.executeQuery("SELECT * FROM revisions")
    while (result.next()) {
      objects ++ Map(
        ("title", result.getString("title")),
        ("id", result.getInt("id"))
      )
    }
    objects
  }

}
