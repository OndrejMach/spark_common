package com.tmobile.sit.common.readers

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

//case class myCSV(id: Int, name: String)

private[readers] abstract class CsvGenericReader extends Logger{
  def getCSVReader(header: Boolean,
                   badRecordsPath: Option[String] = None,
                   delimiter: String = ",",
                   quote: String = "\"",
                   escape: String = "\\",
                   encoding: String = "UTF-8",
                   schema: Option[StructType] = None)(implicit sparkSession: SparkSession): DataFrameReader  = {
    val reader = sparkSession
      .read
      .option("header", if (header) "true" else "false")
      .option("quote", quote )
      .option("escape", escape)
      .option("delimiter", delimiter)
      .option("encoding",encoding )

    val invalidHandling = if (badRecordsPath.isDefined) {
      logger.info(s"Bad records to be stored in ${badRecordsPath.get}")
      reader.option("badRecordsPath", badRecordsPath.get)
    }
    else reader.option("mode", "DROPMALFORMED")

    if (schema.isDefined) {
      invalidHandling.schema(schema.get)
    } else {
      logger.warn("Schema file not defined, trying to infer one")
      invalidHandling.option("inferschema", "true")
    }
  }
}

class CSVReader(path: String,
                header: Boolean,
                badRecordsPath: Option[String] = None,
                delimiter: String = ",",
                quote: String = "\"",
                escape: String = "\\",
                encoding: String = "UTF-8",
                schema: Option[StructType] = None)
               (implicit sparkSession: SparkSession) extends CsvGenericReader with Reader {

  private def getCsvData(path: String): DataFrame = {
    logger.info(s"Reading CSV from path ${path}")
    val reader = getCSVReader(header, badRecordsPath, delimiter, quote, escape, encoding, schema)
    reader.csv(path)

  }

  override def read(): DataFrame = getCsvData(path)
}

class CSVMultifileReader(path: String, fileList: Seq[String],
                                 header: Boolean,
                                 badRecordsPath: Option[String] = None,
                                 delimiter: String = ",",
                                 quote: String = "\"",
                                 escape: String = "\\",
                                 encoding: String = "UTF-8",
                                 schema: Option[StructType] = None)
                                (implicit sparkSession: SparkSession) extends CsvGenericReader  with Reader {
  private def getCsvData(path: String): DataFrame = {
    logger.info(s"Reading CSV from path ${path}")
    val reader = getCSVReader(header, badRecordsPath, delimiter, quote, escape, encoding, schema)
    reader.csv(fileList.map(path+"/"+_) :_*)

  }

  override def read(): DataFrame = getCsvData(path)
}

object CSVReader {
  def apply(path: String,
            header: Boolean,
            badRecordsPath: Option[String] = None,
            delimiter: String = ",",
            quote: String = "\"",
            escape: String = "\\",
            encoding: String = "UTF-8",
            schema: Option[StructType] = None)(implicit sparkSession: SparkSession): CSVReader =

    new CSVReader(path, header, badRecordsPath, delimiter, quote, escape, encoding, schema)(sparkSession)
}

object CSVMultifileReader {
  def apply(path: String,
            fileList: Seq[String],
            header: Boolean,
            badRecordsPath: Option[String] = None,
            delimiter: String = ",",
            quote: String = "\"",
            escape: String = "\\",
            encoding: String = "UTF-8",
            schema: Option[StructType] = None)
           (implicit sparkSession: SparkSession): CSVMultifileReader =

    new CSVMultifileReader(path, fileList, header, badRecordsPath, delimiter, quote, escape, encoding, schema)(sparkSession)
}