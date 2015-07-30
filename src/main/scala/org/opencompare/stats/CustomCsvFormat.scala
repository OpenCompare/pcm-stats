package org.opencompare.stats

import com.github.tototoshi.csv.{QUOTE_ALL, DefaultCSVFormat}

/**
 * Created by smangin on 23/07/15.
 */
class CustomCsvFormat extends DefaultCSVFormat {
  override val delimiter = ','
  override val quoteChar = '"'
  override val quoting = QUOTE_ALL
  override val treatEmptyLineAsNil = true
}
