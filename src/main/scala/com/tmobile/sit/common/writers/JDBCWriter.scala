package com.tmobile.sit.common.writers

import java.util.Properties

import org.apache.spark.sql.DataFrame

/**
 * Writer capable of writing into generally any JDBC accessible relational database. In order to be able to do that JDBC driver must be provided in the classpath.
 * @author Ondrej Machacek
 * @param data - DataFrame to write
 * @param url - Database JDBC connection URL
 * @param table - Table to write into
 * @param connectionProperties - General connection properties (credentials etc.)
 */

class JDBCWriter(data: DataFrame,url: String, table: String, connectionProperties: Properties) extends Writer {
  def writeData(): Unit = {
    logger.info(s"Writing via JDBC to ${url} table: ${table}")
    data
      .write
      .jdbc(url, table, connectionProperties)
  }
}

object JDBCWriter {
  def apply(data: DataFrame, url: String, table: String, connectionProperties: Properties): JDBCWriter = new JDBCWriter(data,url, table, connectionProperties)
}
