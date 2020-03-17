package com.tmobile.sit.common

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.tmobile.sit.common.writers.CSVMultifileWriter
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WritersTest extends FlatSpec with DataFrameSuiteBase  {
  implicit lazy val _ : SparkSession = spark

  "csvMultifileWriter" should "Write csvs properly" in {
    import spark.implicits._

    val data = Map("tmp/file1.csv" -> ReferenceData.multi_csv.toDF(), "tmp/file2.csv"->ReferenceData.multi_csv.toDF() )
   CSVMultifileWriter(data).writeData()
  }


}