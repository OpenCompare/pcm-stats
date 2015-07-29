package org.opencompare.stats

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.rosuda.JRI.Rengine

/**
 * Created by blacknight on 29/07/15.
 */
object Metrics extends App {

  private val rengine = new Rengine()
  val cTime = LocalDateTime.now()
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def process(): Unit = {
    rengine.startMainLoop()
    val data = rengine.eval("read.csv(file=\"" + "metrics/pcm/" + cTime.format(formatter) + "_pcmMetrics.csv" + "\", header=TRUE, sep=\",\")").asMatrix()

    rengine.end()
  }

  process()

}
