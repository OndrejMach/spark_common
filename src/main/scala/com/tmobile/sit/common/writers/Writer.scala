package com.tmobile.sit.common.writers

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.DataFrame

trait Writer extends Logger {
  def writeData(data: DataFrame) : Unit
}
