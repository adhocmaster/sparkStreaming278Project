package com.adhocmaster.scala.fileStreamer

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import scala.io.Source
import scala.io.StdIn
import scala.util.Random

import org.apache.log4j.Logger

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
    val cleanOutputDir = args( 3 ).toBoolean

    if ( cleanOutputDir ) {
      cleanDir( outputStreamDir )
    }

    createFiles( outputStreamDir, inputFilePath, totalStreamTimeSeconds )
    logger.warn( "Main thread finished" )
  }

  def createOutputDir( outputStreamDir: String ) = {

    val outputPath = Paths.get( outputStreamDir )
    if ( !Files.exists( outputPath ) )
      Files.createDirectories( outputPath )

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
    val entries = index.list();
    for ( s <- entries ) {
      val currentFile = new File( index.getPath(), s );
      currentFile.delete();
    }

  }

/***
   * Files will be created randomly in the period of totalStreamTimeSeconds
   */
  def createFiles( outputPath: String, inputFilePath: String, totalStreamTimeSeconds: Int ) = {

    print( s"the file streamer will run for ~${totalStreamTimeSeconds * 1000}ms. Press enter to continue:" )
    val response = StdIn.readLine()

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

      logger.debug( s"creating next thread $fileNo" )

      val wait: Long = Random.nextInt( totalStreamTimeSeconds ) * 1000
      val thread = new FileThread( wait, outputFilePath, items )
      executor.submit( thread )

      fileNo += 1
      outputFilePath = outputPath + "/part_" + fileNo

    }

    executor.shutdown()
    executor.awaitTermination( totalStreamTimeSeconds, TimeUnit.SECONDS )

    logger.warn( "Existing App" )

  }
}
