package com.tmobile.sit.common.readers

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class for reading general relational database accessible via JDBC. Class implemented Reader to make testing and integration easier.
 * In order to be able to do that JDBC driver must be provided in the classpath.
 * @author Ondrej Machacek
 *
 * @param url - URL of the Database connector
 * @param table - table to read
 * @param properties - used for credentials etc..
 * @param sparkSession - implicit SparkSession for the reading itself
 */

class JDBCReader(url: String, table: String, properties: Properties)(implicit sparkSession: SparkSession) extends Reader {
  def read() : DataFrame = {
    sparkSession
      .read
      .jdbc(url, table, properties)
  }
}
