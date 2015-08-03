package org.opencompare.stats.utils

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{Layout, SimpleLayout}

/**
 * Created by smangin on 03/08/15.
 */
class CustomLoggerLayout extends SimpleLayout {

  override def format(event : LoggingEvent): String = {
    val cTime = LocalDateTime.now()
    val fullFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,S")
    val sbuf = new StringBuffer(128);
    sbuf.setLength(0);
    sbuf.append(cTime.format(fullFormatter));
    sbuf.append(" -- ");
    sbuf.append(event.getLevel().toString());
    sbuf.append(" -- ");
    sbuf.append(event.getLoggerName);
    sbuf.append(" -- ");
    sbuf.append(event.getRenderedMessage());
    sbuf.append(Layout.LINE_SEP);
    sbuf.toString();
  }

}
