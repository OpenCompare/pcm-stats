package org.opencompare.stats.utils

import org.opencompare.io.wikipedia.io.{WikiTextTemplateProcessor, MediaWikiAPI}

/**
 * Created by gbecan on 6/11/15.
 */
class WikiTextKeepTemplateProcessor(mediaWikiAPI : MediaWikiAPI, initialCache : Map[String, String] = Map.empty[String, String]) extends WikiTextTemplateProcessor(mediaWikiAPI, initialCache) {

  override def expandTemplate(language : String, template: String): String = {
    template
  }

}
