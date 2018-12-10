package com.adhocmaster.cmps278.spark

import org.apache.spark.sql.SparkSession
import com.adhocmaster.cmps278.spark.util.ConfigurationManager

/**
 * @author ${user.name}
 */
object App {

  def main( args: Array[String] ) {

    ConfigurationManager.load()
    println( ConfigurationManager.toString )

    val spark = SparkSession.builder()
      .appName( "The swankiest Spark app ever" )
      .master( "local[*]" )
      .getOrCreate()

    val sc = spark.sparkContext

    val col = sc.parallelize( 0 to 100 by 5 )
    val smp = col.sample( true, 4 )
    val colCount = col.count
    val smpCount = smp.count

    println( "orig count = " + colCount )
    println( "sampled count = " + smpCount )

  }

}
