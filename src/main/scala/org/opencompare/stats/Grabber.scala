package org.opencompare.stats

import java.util.Date
import java.text.DateFormat
import java.util
import java.util.Locale

import org.opencompare.io.wikipedia.io.MediaWikiAPI
import play.api.libs.json.JsValue

import scala.collection.JavaConverters._
;

/**
 * Created by smangin on 7/23/15.
 *
 * Use to get all revision from a single wikipedia page
 *
 */
class Grabber (lang : String, title : String){


    val api : MediaWikiAPI = new MediaWikiAPI("https", "wikipedia.org")
    val result = api.getVersionFromTitle(lang, title)

    def getRevIds(): util.List[String] = {
        val revids: util.List[String] = new util.ArrayList();
        for (value: JsValue <- result) {
            var revid = value \ "revid"
            revids.add(revid.toString)
        }
        revids
    }

    def getDate(revid : String): Date = {
        var timestamp = ""
        for (value : JsValue <- result) {
            if ((value \ "revid").equals(revid)) {
                timestamp = (value \ "timestamp").toString
            }
        }
        if (timestamp != "") {
            val df = DateFormat.getDateInstance(DateFormat.LONG, Locale.FRANCE);
            df.parse(timestamp)
        } else {
            new Date()
        }
    }

    def getAuthor(revid : String): String = {
        var author = ""
        for (value : JsValue <- result) {
            if ((value \ "revid").equals(revid)) {
                author = (value \ "user").toString
            }
        }
        author
    }
}
