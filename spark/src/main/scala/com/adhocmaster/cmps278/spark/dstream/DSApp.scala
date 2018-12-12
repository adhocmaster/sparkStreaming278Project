package com.adhocmaster.cmps278.spark.dstream

import org.apache.log4j.Logger
import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import com.adhocmaster.cmps278.spark.util.ConfigurationManager
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner
import scala.util.control.Breaks._

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
  var fileStream: DStream[String] = null

  def run = {

    // 1.
    loadConfigurations

    // 2.
    fileStream = ssc.textFileStream( inputDir )
    //    val fileStream = ssc.textFileStream( "F:/myProjects/cmps278/data/filestreamdir" )

    // 3.
    createNameHistoryStream( fileStream )

    // 4.
    if ( storageLevel != StorageLevel.NONE ) {

      nameHistoryStream.persist( storageLevel )
      logger.warn( s"nameHistoryStream persisted with $storageLevel" )

    }

    // 5. get operation results
    val results = operation match {

      case "countByName"                  => countByName
      case "countByNameSort"              => countByNameSort
      case "countByNameRepartitionedSort" => countByNameRepartitionedSort
      case "countByNameSortByNumber"      => countByNameSortByNumber
      case "top100Names"                  => top100Names
      case "top100NamesWithPartitionSort" => top100NamesWithPartitionSort
      case _                              => throw new NotImplementedError( s"$operation not implemented" )

    }

    // 6. save output
    val repart = results.repartition( 1 )

    val count = repart.count()

    logger.warn( s"Saving output at $outputDir/DSApp.txt" )
    logger.warn( s"Number of items in part: ${count}" )

    try {
      repart.saveAsTextFiles( outputDir + "/DSApp", "txt" )
    } catch {
      case e: Throwable => logger.warn( s"could not save file in output director" )
    }

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
  def countByNameSort = {

    nameHistoryStream.map( h => ( h.name, h.number ) ).reduceByKey( ( c1, c2 ) => c1 + c2 ).transform( ( rdd, time ) => {
      rdd.sortByKey( false, 1 )
    } )

  }
  def countByNameRepartitionedSort = {

    import spark.implicits._
    nameHistoryStream.map( h => ( h.name, h.number ) ).reduceByKey( ( c1, c2 ) => c1 + c2 ).transform( ( rdd, time ) => {
      rdd.repartitionAndSortWithinPartitions( new HashPartitioner( 100 ) )
    } )

  }

  def countByNameSortByNumber = {

    nameHistoryStream.map( h => ( h.name, h.number ) ).reduceByKey( ( c1, c2 ) => c1 + c2 ).transform( ( rdd, time ) => {
      rdd.sortBy( _._2, false, 600 )
    } )

  }

  def top100Names = {

    val totalState = countByName.updateStateByKey( ( newVals: Seq[Int], countOptional: Option[Int] ) => {
      countOptional match {
        case Some( total ) => Some( newVals.sum + total )
        case None          => Some( newVals.sum )
      }

    } )

    totalState.transform( ( rdd, time ) => {

      val list = rdd.sortBy( _._2, false ).take( 100 )
      rdd.filter( t => {

        var found: Boolean = false
        breakable {

          for ( item <- list ) {
            if ( item._1 == t._1 ) {
              found = true
              break
            }
          }
        }

        found

      } )

    } )

  }

  def top100NamesWithPartitionSort = {

    val totalState = countByName.updateStateByKey( ( newVals: Seq[Int], countOptional: Option[Int] ) => {
      countOptional match {
        case Some( total ) => Some( newVals.sum + total )
        case None          => Some( newVals.sum )
      }

    } )

    totalState.transform( ( rdd, time ) => {

      val list = rdd.keyBy( t => ( t._2, t._1 ) ).repartitionAndSortWithinPartitions( new HashPartitioner( 10 ) ).take( 100 )
      rdd.filter( list.contains )

    } )
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
