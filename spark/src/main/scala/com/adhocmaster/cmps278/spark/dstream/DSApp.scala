package com.adhocmaster.cmps278.spark.dstream

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Logger
import java.sql.Timestamp

class DSApp( spark: SparkSession, sc: SparkContext, ssc: StreamingContext, inputDir: String ) {

  val logger = Logger.getLogger( getClass.getName )

  def run = {

    val fileStream = ssc.textFileStream( inputDir )
    //    val fileStream = ssc.textFileStream( "F:/myProjects/cmps278/data/filestreamdir" )

    val nameHistoryStream = fileStream.flatMap( line => {

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
}
