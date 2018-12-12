package com.adhocmaster.cmps278.spark

import org.apache.spark.sql.SparkSession
import com.adhocmaster.cmps278.spark.util.ConfigurationManager
import com.adhocmaster.cmps278.spark.data.BabyNames
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.streaming._
import com.adhocmaster.cmps278.spark.dstream.DSApp
import java.io.File
import scala.io.StdIn
import org.specs2.reporter.stdOut
import java.util.Calendar
import org.apache.commons.io.FileUtils
import java.nio.file.Files
import java.nio.file.Paths

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

    logger.warn( "Existing App" )

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
    val cleanOutputDir = ConfigurationManager.getVal( "data.destination.clean" ).get.toBoolean

    if ( cleanOutputDir ) {
      cleanDir( outputDir )
    } else {
      logger.warn( "Output directory not clean" )
    }

    if ( streamingType == "DStream" ) {

      val dsApp = new DSApp( spark, sc, ssc, inputDir, outputDir, streamingOperation )
      dsApp.run

    }

  }

  def runStreamer = {

    val timeoutInMilliseconds = ConfigurationManager.getVal( "streaming.timeoutInMilliseconds" ).get.toLong

    print( s"the streamer will run for ~${timeoutInMilliseconds}ms. Press any enter to continue:" )
    val response = StdIn.readLine()

    val start = System.currentTimeMillis()
    ssc.start()
    ssc.awaitTerminationOrTimeout( timeoutInMilliseconds + 5000 ) // some time to start file streaming

    //Thread.sleep( timeoutInMilliseconds ) //
    ssc.stop( false )

    val end = System.currentTimeMillis()
    logger.warn( s"Stream execution time ${end - start}ms" )

  }

  def cleanDir( dir: String ): Unit = {

    print( s"Are you sure to clean $dir?[y/n]:" )
    val response = StdIn.readLine()
    if ( response.trim != "y" ) {

      logger.warn( s"skipping cleaning $dir by user interaction" )
      return

    }

    logger.warn( s"cleaning directory $dir" )
    val index = new File( dir );
    FileUtils.deleteDirectory( index )

    val outputPath = Paths.get( dir )
    if ( !Files.exists( outputPath ) )
      Files.createDirectories( outputPath )

  }
}
