package com.adhocmaster.cmps278.spark

import org.apache.spark.sql.SparkSession
import com.adhocmaster.cmps278.spark.util.ConfigurationManager
import com.adhocmaster.cmps278.spark.data.BabyNames
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.streaming._
import com.adhocmaster.cmps278.spark.dstream.DSApp

/**
 * @author ${user.name}
 */
object App {

  val logger = Logger.getLogger( getClass.getName )
  var spark: SparkSession = null
  var sc: SparkContext = null
  var ssc: StreamingContext = null

  def main( args: Array[String] ) {

    init
    runStreamApp
    runStreamer

  }

  def init = {

    ConfigurationManager.load()
    println( ConfigurationManager.toString )

    spark = SparkSession.builder()
      .appName( "The swankiest Spark app ever" )
      .master( "local[*]" )
      .getOrCreate()

    sc = spark.sparkContext
    ssc = new StreamingContext( sc, Milliseconds( ConfigurationManager.getVal( "streaming.intervalInMilliseconds" ).get.toLong ) )

    //    BabyNames.loadAsDF( spark )

  }

  def runStreamApp = {

    val streamingType = ConfigurationManager.getVal( "streaming.type" ).get
    val streamingOperation = ConfigurationManager.getVal( "streaming.operation" ).get
    val inputDir = ConfigurationManager.getVal( "data.source.stream" ).get
    val outputDir = ConfigurationManager.getVal( "data.destination.stream" ).get

    if ( streamingType == "DStream" ) {

      val dsApp = new DSApp( spark, sc, ssc, inputDir, outputDir, streamingOperation )
      dsApp.run

    }

  }

  def runStreamer = {

    val timeoutInMilliseconds = ConfigurationManager.getVal( "streaming.timeoutInMilliseconds" ).get.toLong
    ssc.start()
    ssc.awaitTerminationOrTimeout( timeoutInMilliseconds )

    Thread.sleep( timeoutInMilliseconds + 2000 ) //

  }

}
