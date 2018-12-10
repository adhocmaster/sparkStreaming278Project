package com.adhocmaster.cmps278.spark.util

import java.io._
import scala.io.Source

object ConfigurationManager {

  var confs: Map[String, String] = Map()

  def load() {

    val stream: InputStream = getClass.getResourceAsStream( "/application.properties" )
    val lines = Source.fromInputStream( stream ).getLines

    val tuples = lines.map( line => {

      println( line )

      val arr = line.split( "=" )

      ( arr( 0 ).trim, arr( 1 ).trim )

    } )

    confs = tuples.toMap

  }

  def getVal( name: String ): Option[String] = {

    confs.get( name )

  }

  override def toString = {

    confs.toString()

  }

}
