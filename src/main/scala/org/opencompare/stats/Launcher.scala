package org.opencompare.stats

import java.io.InputStreamReader

import com.opencsv.CSVReader
import play.libs.Json

/**
 * Created by smangin on 23/07/15.
 */
object Launcher {

  def main(args: Array[String]) {
    val input = getClass.getResourceAsStream("/list_of_PCMs.csv")
    val reader = new CSVReader(new InputStreamReader(input), '"', ',')
    //for (line <- reader.readAll()) {
    //  println(line)
    //}
    val grabber = new Grabber("en", "Comparison_(grammar)")
    for (revid <- grabber.getRevIds) {
      println(revid)
      println(grabber.getDate(revid))
      println(grabber.getAuthor(revid))
    }
  }
}
