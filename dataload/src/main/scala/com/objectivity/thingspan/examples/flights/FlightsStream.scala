package com.objectivity.thingspan.examples.flights
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.streaming.Seconds

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser

object FlightsStream {

	def main(args: Array[String]) {
		var options = new Options();
		options.addOption("b", "boot", true, "Boot file");
		options.addOption("h", "host", true, "Host name");
		options.addOption("p", "port", true, "Port");
		options.addOption("t", "time", true, "Time");
		options.addOption("m", "master", true, "Master URL");

		val parser = new PosixParser()
		val cl = parser.parse(options, args, false)



		if (cl.hasOption("b")){
			val bootString = cl.getOptionValue("b", "data/flights.boot")
					AppConfig.Boot = bootString
		}	
		
		if (cl.hasOption("h")){
			AppConfig.host = cl.getOptionValue("h", "localhost")
		}	
		
		if (cl.hasOption("p")){
			AppConfig.port = cl.getOptionValue("p", "7777").toInt
		}	

		if (cl.hasOption("t")){
			AppConfig.time = cl.getOptionValue("t", "30").toInt
		}	

	  val sparkConf = new SparkConf().setAppName("FlightsStream")
	  if (cl.hasOption("m")){
			sparkConf.setMaster(cl.getOptionValue("m", "local[*]"))
		}	

		val sc = new SparkContext(sparkConf)

		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._

//		val flightsDF = sqlContext.load("jdbc", Map(
//				"url" -> "jdbc:oracle:thin:blog_refdata/password@bigdatalite.rittmandev.com:1521:orcl",
//				"dbtable" -> "BLOG_REFDATA.POST_DETAILS"))
//
//		flightsDF.registerTempTable("flightsTable")

		val streamingContext = new StreamingContext(sc, Seconds(AppConfig.time))

		val flightsCSVDStream = streamingContext.textFileStream("file:///Users/peter/git/thingspan-flights/data/flights")

		val flightsDStream = flightsCSVDStream.map(Flight.flightFromString(_))
		
		val windowDStream = flightsDStream.window(Seconds(AppConfig.time), Seconds(AppConfig.time * 2))

		windowDStream.foreachRDD(flightsGroup => {
		  val flightsGroupCount = flightsGroup.count() 
			if (flightsGroupCount == 0) {
				println("No fights received in this time interval")
			} else {
			  println(s"Saving $flightsGroupCount to Thingspan")
			  var start = java.lang.System.currentTimeMillis()
				val flightsDF = flightsGroup.toDF()
  			println("Flights schema:")
  			flightsDF.printSchema()
//  			flightsDF.write.mode(SaveMode.Overwrite).
//  			format("com.objy.spark.sql").
//  			option("objy.bootFilePath", AppConfig.Boot).
//  			option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
//  			save()
        var stop = java.lang.System.currentTimeMillis()
        var totalTime = (stop-start)
				println(s"$flightsGroupCount saved in $totalTime ms")
			}
		})

		streamingContext.start()
		streamingContext.awaitTermination()
	}
}