package com.tmobile.sit.common.readers

import com.crealytics.spark.excel._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * A reader class for getting data from MS Excel files. Should support all the formats. Inherits from Reader trait to achieve better testability.
 * Reads a single excel file.
 * @author Ondrej Machacek
 *
 * @param path - where excel file is located
 * @param sheetName - name of the sheet to read from
 * @param cellRange - cell range to read
 * @param schema - you can explicitly specify data structure (and types) using spark types. Highly recommended to use this one. If not specified schema is inferred.
 * @param dateFormat  - format for the dates
 * @param sparkSession - implicit SparkSession used for file reading.
 */

class ExcelReader(path: String,
                  sheetName: String = "",
                  cellRange: String = "!A1",
                  schema: Option[StructType] = None,
                  dateFormat: String = "yyyy-mm-dd hh:mm:ss")
                 (implicit sparkSession: SparkSession) extends Reader {
  override def read(): DataFrame = {
    logger.info(s"Reading excel file from path ${path}, sheet-cellRange: ${sheetName}-${cellRange}")
    val dfReader = sparkSession.read
      .format("com.crealytics.spark.excel")
      .option("dataAddress", s"${sheetName}${cellRange}") // Optional, default: "A1"
      .option("useHeader", "true") // Required
      .option("treatEmptyValuesAsNulls", "true") // Optional, default: true
      .option("timestampFormat", dateFormat) // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]

    val withSchema = if (schema.isDefined) {
      dfReader
        .schema(schema.get)
        .option("inferSchema", "false")
    } else {
      logger.warn("Schema not defined, trying to infer one")
      dfReader.option("inferSchema", "true")
    }

    withSchema
      .load(path)
  }
}

/**
 * Companion object for Excel reader.
 */
object ExcelReader {
  def apply(path: String,
            sheetName: String = "",
            cellRange: String = "!A1",
            schema: Option[StructType] = None,
            dateFormat: String = "yyyy-mm-dd hh:mm:ss")(implicit sparkSession: SparkSession): ExcelReader =
    new ExcelReader(path, sheetName, cellRange, schema, dateFormat)(sparkSession)
}