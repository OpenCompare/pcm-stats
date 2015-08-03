package org.opencompare.stats.utils

import com.github.tototoshi.csv.{DefaultCSVFormat, QUOTE_ALL}

/**
 * Created by smangin on 23/07/15.
 */
class CustomCsvFormat extends DefaultCSVFormat {
  override val delimiter = ','
  override val quoteChar = '"'
  override val quoting = QUOTE_ALL
  override val treatEmptyLineAsNil = true
}
