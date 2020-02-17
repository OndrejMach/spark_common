package com.tmobile.sit.common.writers

import java.util.Properties

import org.apache.spark.sql.DataFrame

class JDBCWriter(url: String, table: String, connectionProperties: Properties) extends Writer {
  def writeData(data: DataFrame): Unit = {
    logger.info(s"Writing via JDBC to ${url} table: ${table}")
    data
      .write
      .jdbc(url, table, connectionProperties)
  }
}
