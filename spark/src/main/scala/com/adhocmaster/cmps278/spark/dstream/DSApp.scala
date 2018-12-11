package com.adhocmaster.cmps278.spark.dstream

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext

class DSApp( spark: SparkSession, sc: SparkContext, ssc: StreamingContext, inputDir: String ) {

  def run = {

    val fileStream = ssc.textFileStream( inputDir )

  }
}
