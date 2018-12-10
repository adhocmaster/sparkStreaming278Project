package com.adhocmaster.cmps278.spark.data

import com.adhocmaster.cmps278.spark.util.ConfigurationManager
import org.apache.spark.sql._
import com.adhocmaster.cmps278.spark.exceptions.NotFoundException
import org.apache.log4j.Logger

object BabyNames {

  val logger = Logger.getLogger( getClass.getName )

  var df: DataFrame = null

  def loadAsDF( spark: SparkSession ) = {

    val source = ConfigurationManager.getVal( "data.source" )

    if ( source == None ) {

      logger.error( "data.source not found in properties" )
      throw new NotFoundException( "data.source not found in properties" )

    }

    df = spark.read
      .format( "csv" )
      .option( "header", "true" )
      .option( "inferSchema", "true" )
      .load( source.get )

    logger.info( s"Baby names loaded from ${source.get}" )

  }

}
