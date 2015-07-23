package org.opencompare.io.wikipedia.io

import java.util.regex.Matcher

import play.api.libs.json.{JsString, Json}

import scalaj.http.Http

/**
 * Created by gbecan on 6/19/15.
 */
class MediaWikiAPI(
                  val protocol : String = "https",
                  val baseServer : String
                    ) {

  // Constructor for Java compatibility with optional parameters
  def this(initBaseServer : String) {
    this("https", initBaseServer)
  }

  private def apiEndPoint(language : String) : String = {
    protocol + "://" + language + "." + baseServer + "/w/api.php"
  }

  private def escapeTitle(title : String) : String = {
    title.replaceAll("\\s", "_")
  }

  def getVersionFromTitle(language : String, title : String): String = {
    //Example: https://en.wikipedia.org/w/api.php?action=query&format=json&prop=revisions&titles=Comparison_of_AMD_processors&rvprop=content
    val result = Http(apiEndPoint(language)).params(
      "action" -> "query",
      "format" -> "json",
      "prop" -> "revisions",
      "titles" -> escapeTitle(title),
      "rvprop" -> "content"
    ).asString.body

    val jsonResult = Json.parse(result)
    val jsonWikitext = jsonResult \ "query" \ "pages" \\ "*"

    if (jsonWikitext.nonEmpty) {
      jsonWikitext.head.as[JsString].value
      //      Json.stringify(jsonWikitext.head)
      //        .replaceAll(Matcher.quoteReplacement("\\n"), "\n")
      //        .replaceAll(Matcher.quoteReplacement("\\\""), "\"")
    } else {
      // TODO: Error
      ""
    }
  }

  def getWikitextFromTitle(language : String, title : String): String = {
    //Example: https://en.wikipedia.org/w/api.php?action=query&format=json&prop=revisions&titles=Comparison_of_AMD_processors&rvprop=content
    val result = Http(apiEndPoint(language)).params(
      "action" -> "query",
      "format" -> "json",
      "prop" -> "revisions",
      "titles" -> escapeTitle(title),
      "rvprop" -> "content"
    ).asString.body

    val jsonResult = Json.parse(result)
    val jsonWikitext = jsonResult \ "query" \ "pages" \\ "*"

    if (jsonWikitext.nonEmpty) {
      jsonWikitext.head.as[JsString].value
//      Json.stringify(jsonWikitext.head)
//        .replaceAll(Matcher.quoteReplacement("\\n"), "\n")
//        .replaceAll(Matcher.quoteReplacement("\\\""), "\"")
    } else {
      // TODO: Error
      ""
    }
  }

  def expandTemplate(language : String, template : String) : String = {
    val result = Http(apiEndPoint(language)).params(
      "action" -> "expandtemplates",
      "format" -> "json",
      "prop" -> "wikitext",
      "text" -> template
    ).asString.body

    val jsonResult = Json.parse(result)
    val jsonExpandedTemplate = (jsonResult \ "expandtemplates" \ "wikitext").toOption
    val expandedTemplate = if (jsonExpandedTemplate.isDefined) {
      jsonExpandedTemplate.get match {
        case s : JsString => s.value
        case _ => ""
      }
    } else {
      ""
    }

    expandedTemplate
  }


}
