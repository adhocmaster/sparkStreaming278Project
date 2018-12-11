package com.adhocmaster.scala.fileStreamer

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import java.nio._
import java.nio.file._
import scala.sys
import java.io.BufferedWriter
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import org.spark_project.guava.collect.Iterators
import java.util.Collections
import scala.util.Random
import scala.math
import scala.io.Source
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * @author ${user.name}
 */
object App {

  val logger = Logger.getLogger( getClass.getName )

  val linesPerOutputFile = 10000
  var totalStreamTimeSeconds = 10

  def main( args: Array[String] ) {

    val inputFilePath = args( 0 )
    val outputStreamDir = args( 1 )
    val totalStreamTimeSeconds = args( 2 ).toInt

    val spark = SparkSession.builder()
      .appName( "The swankiest Spark app ever" )
      .master( "local[*]" )
      .getOrCreate()

    val sc = spark.sparkContext
    createOutputDir( outputStreamDir )

    createFiles( outputStreamDir, inputFilePath, totalStreamTimeSeconds )
    logger.warn( "Main thread finished" )
  }

  def createOutputDir( outputStreamDir: String ) = {

    val outputPath = Paths.get( outputStreamDir )
    if ( !Files.exists( outputPath ) )
      Files.createDirectories( outputPath )

  }

/***
   * Files will be created randomly in the period of totalStreamTimeSeconds
   */
  def createFiles( outputPath: String, inputFilePath: String, totalStreamTimeSeconds: Int ) = {

    Random.setSeed( 0 ) // required for reproduction

    val lines = Source.fromFile( inputFilePath ).getLines.toList
    logger.warn( s"number of lines ${lines.count( _ => true )}" )
    val noFiles: Int = lines.size / linesPerOutputFile

    var fileNo = 1
    var outputFilePath = outputPath + "/part_" + fileNo

    val executor: ExecutorService = Executors.newFixedThreadPool( 20 )
    for ( i <- 1 to noFiles ) {

      val items = lines.take( linesPerOutputFile )
      lines.drop( linesPerOutputFile )

      logger.warn( s"creating next thread $fileNo" )

      val wait: Long = Random.nextInt( totalStreamTimeSeconds ) * 1000
      val thread = new FileThread( wait, outputFilePath, items )
      executor.submit( thread )

      fileNo += 1
      outputFilePath = outputPath + "/part_" + fileNo

    }

    executor.shutdown()
    executor.awaitTermination( totalStreamTimeSeconds * 2, TimeUnit.SECONDS )

  }
}
