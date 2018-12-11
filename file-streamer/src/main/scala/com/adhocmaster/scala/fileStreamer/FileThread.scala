package com.adhocmaster.scala.fileStreamer

import org.apache.log4j.Logger
import java.nio._
import java.nio.file._
class FileThread( wait: Long, outputFilePath: String, items: List[String] ) extends Runnable {

  val logger = Logger.getLogger( getClass.getName )

  override def run {

    // 1. wait for some time

    logger.warn( s"Waiting for $wait mili seconds before publishing file $outputFilePath" )
    Thread.sleep( wait )

    logger.warn( s"writing to $outputFilePath " )
    Files.write( Paths.get( outputFilePath ), items.mkString( sys.props( "line.separator" ) ).getBytes, StandardOpenOption.CREATE )

  }
}
