package org.opencompare.stats

import java.io.{FileWriter, File}

import com.github.tototoshi.csv.{CSVReader, CSVWriter, DefaultCSVFormat, QUOTE_ALL}

/**
 * Created by smangin on 23/07/15.
 */
object Launcher extends App{
  new File("src/main/resources/revisions/wikitext/").mkdirs()

  implicit object MyFormat extends DefaultCSVFormat {
    override val delimiter = ','
    override val quoteChar = '"'
    override val quoting = QUOTE_ALL
  }

  val reader = CSVReader.open("src/main/resources/list_of_PCMs.csv")(MyFormat)

  val output = new File("src/main/resources/revisions/revisions.csv")
  output.createNewFile()
  val writer = CSVWriter.open(output)(MyFormat)
  writer.writeRow(List("Title", "Lang", "Id", "Date", "Author"))

  reader.all().foreach(line => {
    val pageLang = line.apply(0)
    val pageTitle = line.apply(1)
    println("### " + pageTitle + ":" + pageLang)
    val grabber = new Revision(pageLang, pageTitle)
    print("\t rev : ")
    for (revid <- grabber.getIds) {
      print(revid + ", ")
      writer.writeRow(List(pageTitle, pageLang, revid.toString, grabber.getDate(revid).get, grabber.getAuthor(revid)))
      writer.flush()
      val wikiWriter = new FileWriter(new File("src/main/resources/revisions/wikitext/" + pageTitle + "-" + revid + ".wikitext"))
      wikiWriter.write(grabber.getWikitext(revid))
      wikiWriter.flush()
      wikiWriter.close()
    }
    println()
  })
  writer.close()
}
