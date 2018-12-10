package com.adhocmaster.cmps278.spark

import org.apache.spark.sql.SparkSession
import com.adhocmaster.cmps278.spark.util.ConfigurationManager
import com.adhocmaster.cmps278.spark.data.BabyNames
import org.apache.log4j.Logger

/**
 * @author ${user.name}
 */
object App {

  val logger = Logger.getLogger( getClass.getName )

  def main( args: Array[String] ) {

    init

  }

  def init = {

    ConfigurationManager.load()
    println( ConfigurationManager.toString )

    val spark = SparkSession.builder()
      .appName( "The swankiest Spark app ever" )
      .master( "local[*]" )
      .getOrCreate()

    val sc = spark.sparkContext

    BabyNames.loadAsDF( spark )

  }

}
