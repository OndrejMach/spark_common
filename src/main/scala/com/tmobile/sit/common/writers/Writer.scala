package com.tmobile.sit.common.writers

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.DataFrame

/**
 * Writer interface for basically any writer class implemented here.
 */
trait Writer extends Logger {
  def writeData() : Unit
}
