package com.adhocmaster.cmps278.spark.dstream

import org.apache.log4j.Logger
import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import com.adhocmaster.cmps278.spark.util.ConfigurationManager
import org.apache.spark.streaming.dstream.DStream

class DSApp(
  spark:     SparkSession,
  sc:        SparkContext,
  ssc:       StreamingContext,
  inputDir:  String,
  outputDir: String,
  operation: String ) {

  var storageLevel: StorageLevel = StorageLevel.NONE
  val logger = Logger.getLogger( getClass.getName )
  var nameHistoryStream: DStream[NameHistory] = null

  def run = {

    // 1.
    loadConfigurations

    // 2.
    val fileStream = ssc.textFileStream( inputDir )
    //    val fileStream = ssc.textFileStream( "F:/myProjects/cmps278/data/filestreamdir" )

    // 3.
    createNameHistoryStream( fileStream )

    // 4.
    if ( storageLevel != StorageLevel.NONE ) {

      nameHistoryStream.persist( storageLevel )
      logger.warn( s"nameHistoryStream persisted with $storageLevel" )

    }

    // 5.
    val results = operation match {

      case "countByName" => countByName
      case _             => throw new NotImplementedError( s"$operation not implemented" )

    }

    // 6. save output
    val repart = results.repartition( 1 )

    logger.warn( s"Saving output at $outputDir/DSApp.txt" )
    logger.warn( s"Number of items in part: ${repart.count}" )

    repart.saveAsTextFiles( outputDir + "/DSApp", "txt" )

  }

  def createNameHistoryStream( fileStream: DStream[String] ) = {

    nameHistoryStream = fileStream.flatMap( line => {

      val strArr = line.split( "," )

      try {

        List( NameHistory(
          state  = strArr( 0 ).trim,
          gender = strArr( 1 ).trim,
          year   = strArr( 2 ).trim.toInt,
          name   = strArr( 3 ).trim,
          number = strArr( 4 ).trim.toInt ) )

      } catch {

        case e: Throwable =>
          //          logger.warn( s"Wrong line format ($e) + $line" )
          print( s"Wrong line format ($e) + $line" )
          List()

      }

    } )

  }

  def loadConfigurations = {

    val property = ConfigurationManager.getVal( "streaming.persistence.mode" ).get
    storageLevel = property match {

      case "NONE"            => StorageLevel.NONE
      case "MEMORY_ONLY"     => StorageLevel.MEMORY_ONLY
      case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
      case "DISK_ONLY"       => StorageLevel.DISK_ONLY
      case _                 => StorageLevel.NONE

    }
  }

  def countByName = {

    nameHistoryStream.map( h => ( h.name, h.number ) ).reduceByKey( ( c1, c2 ) => c1 + c2 )

  }
  def countByNameRepartition = {

  }

  def countByYear = {

  }

  def countByYearRepartition = {

  }

  def countByGender = {

  }

  def countByGenderRepartition = {

  }

  def predictBirthYearByName = {

  }

  def predictGenderByName = {

  }
  def predictGenderByNameByRepartition = {

  }
}
