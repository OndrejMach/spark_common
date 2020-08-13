package com.tmobile.sit.common.readers

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.{DataFrame, SparkSession}


case object MSAccessJdbcDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:ucanaccess")
  override def quoteIdentifier(colName: String): String = s"[$colName]"
}

class MSAccessSparkReader(path: String, tableName: String)(implicit sparkSession: SparkSession) extends Reader {
  override def read(): DataFrame = {
    JdbcDialects.registerDialect(MSAccessJdbcDialect)

    Class.forName("net.ucanaccess.jdbc.UcanaccessDriver")
    sparkSession.read
      .format("jdbc")
      .option("driver", "net.ucanaccess.jdbc.UcanaccessDriver")
      .option("url", s"jdbc:ucanaccess://${path};memory=false")
      .option("dbtable", tableName)
      .load()
  }
}

object MSAccessSparkReader {
  def apply(path: String, tableName: String)(implicit sparkSession: SparkSession): MSAccessSparkReader
  = new MSAccessSparkReader(path, tableName)(sparkSession)
}
