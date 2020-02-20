package com.tmobile.sit.common.writers

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * Excel writer class writes standard MS format. Implements trait Writer. You can specify a particula sheet and cell range for writing.
 * @author Ondrej Machacek
 * @param filename - resulting excel filename
 * @param sheetName - sheet name to write into
 * @param cellRange - cell range for writing within the sheet specified
 */
class ExcelWriter(filename: String,  sheetName: String = "", cellRange: String = "A1" ) extends Writer {
  override def writeData(data: DataFrame): Unit = {
    logger.info(s"Writing data to ${filename}, sheet: ${sheetName}")
    data
      .coalesce(1)
      .write
      .format("com.crealytics.spark.excel")
      .option("sheetName", s"${sheetName}${cellRange}")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d")
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
      .mode(SaveMode.Overwrite)
      .save(filename)



  }
}
