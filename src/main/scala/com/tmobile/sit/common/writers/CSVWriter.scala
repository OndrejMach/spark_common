package com.tmobile.sit.common.writers

import java.io.File

import com.tmobile.sit.common.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

private[writers] abstract class Merger extends Logger {
  def merge(srcPath: String, dstPath: String): Unit = {
    logger.info(s"Merging spark output ${srcPath} into a single file ${dstPath}")
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    hdfs.delete(new Path(dstPath), true)
    //FileUtil.fullyDelete(new File(dstPath))
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null)
  }
}

/**
 * CSV Writer class. An instance is able to write CSV files according to the class parameters. It implements method writaData which writes
 * input DataFrame. Writer by default creates just one single partition and creates a CSV file, not folder as regular Spark csv writer does.
 * @author Ondrej Machacek
 *
 * @param path - path and filename for the resulting file. Its a regular file, not folder!
 * @param mergeToSingleFile - if true a single csv file is created - no folder (as spark does it) is there - default is true
 * @param delimiter - delimiter used in the file
 * @param writeHeader - if true header is written as the first line
 * @param quote - quoting character
 * @param escape - used escape character
 * @param encoding - file text encoding
 * @param quoteMode - what is encoded basically. Read spark csv writer documentation for details.
 * @param sparkSession - implicit SparkSession for writing.
 */

class CSVWriter(data: DataFrame,
                 path: String, mergeToSingleFile: Boolean = true,
                delimiter: String = ",", writeHeader: Boolean = true,
                quote: String = "\"", escape: String = "\\",
                encoding: String = "UTF-8", quoteMode: String = "MINIMAL",
                timestampFormat: String = "MM/dd/yyyy HH:mm:ss.SSSZZ",
                dateFormat: String = "yyyy-MM-dd", nullValue: String = "")(implicit sparkSession: SparkSession) extends Merger with Writer {


  def writeData() : Unit = {
    logger.info(s"Writing data to ${path} " )
    data
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", if (writeHeader) "true" else "false")
      .option("sep", delimiter)
      .option("quote", quote )
      .option("escape", escape)
      .option("quoteMode",quoteMode )
      .option("encoding", encoding)
      .option("timestampFormat", timestampFormat)
      .option("dateFormat", dateFormat)
      .option("nullValue", nullValue)
      .csv(path+"_tmp")

    if (mergeToSingleFile) merge(path+"_tmp", path)
  }
}

object CSVWriter {
  def apply(data: DataFrame,
             path: String, mergeToSingleFile: Boolean = true,
            delimiter: String = ",",
            writeHeader: Boolean = true,
            quote: String = "\"",
            escape: String = "\\",
            encoding: String = "UTF-8",
            quoteMode: String = "MINIMAL",
            timestampFormat: String = "MM/dd/yyyy HH:mm:ss.SSSZZ",
            dateFormat: String = "yyyy-MM-dd", nullValue: String = "")
           (implicit sparkSession: SparkSession): CSVWriter =

    new CSVWriter(data,path,mergeToSingleFile ,delimiter, writeHeader, quote, escape, encoding, quoteMode, timestampFormat, dateFormat, nullValue)(sparkSession)
}

