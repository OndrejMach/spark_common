package com.tmobile.sit.common.readers

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait Reader extends Logger{
  def read(): DataFrame

  //def readFromPath(path: String): DataFrame
}
