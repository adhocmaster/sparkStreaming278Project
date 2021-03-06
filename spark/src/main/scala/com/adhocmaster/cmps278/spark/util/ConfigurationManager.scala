package com.adhocmaster.cmps278.spark.util

import java.io._
import scala.io.Source
import org.apache.log4j.Logger

object ConfigurationManager {

  val logger = Logger.getLogger( getClass.getName )

  var confs: Map[String, String] = Map()

  def load() {

    val stream: InputStream = getClass.getResourceAsStream( "/application.properties" )
    val lines = Source.fromInputStream( stream ).getLines

    val tuples = lines.filter( line => !( line.trim.isEmpty() || line.trim.startsWith( "#" ) ) )
      .map( line => {

        logger.info( line )

        val arr = line.split( "=" )

        ( arr( 0 ).trim, arr( 1 ).trim )

      } )

    confs = tuples.toMap

  }

  def getVal( name: String ): Option[String] = {

    confs.get( name )

  }

  def getInt( name: String, default: Int ): Int = {

    val conf = confs.get( name )
    if ( conf == None )
      return default

    return conf.get.toInt

  }

  def getLong( name: String, default: Long ): Long = {

    val conf = confs.get( name )
    if ( conf == None )
      return default

    return conf.get.toLong

  }

  def getStr( name: String, default: String ): String = {

    val conf = confs.get( name )
    if ( conf == None )
      return default

    return conf.get

  }

  override def toString = {

    confs.toString()

  }

}
