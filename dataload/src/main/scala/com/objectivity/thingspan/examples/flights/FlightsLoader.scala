package com.objectivity.thingspan.examples.flights

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.SaveMode
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser
import java.io.PrintWriter
import java.io.StringWriter
import java.text.SimpleDateFormat
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


class FlightsLoader {

}

object FlightsLoader {
	def main(args: Array[String]) = {
			var options = new Options();
			options.addOption("d", "data", true, "Data directory");
			options.addOption("b", "boot", true, "Boot file");
			options.addOption("r", "relation", false, "Make relations");

			val parser = new PosixParser()
			val cl = parser.parse(options, args, false)


			if (cl.hasOption("d")){
				val dataDirString = cl.getOptionValue("d", "data")
						AppConfig.DataDirectory = dataDirString
			}	

			if (cl.hasOption("b")){
				val bootString = cl.getOptionValue("b", "data/flights.boot")
						AppConfig.Boot = bootString
			}	


			var conf = new SparkConf()
			conf.setAppName("FlightDataLoader")
			

			// Turn off extra info for serializer exceptions (not working)
			conf.set("spark.serializer.extraDebugInfo", "false")

			val sc = new SparkContext(conf)
			val sqlContext = new SQLContext(sc);
			import sqlContext.implicits._

			if (cl.hasOption("r")){
  			try {
  				println("Creating relationships")
  
  				var start = java.lang.System.currentTimeMillis()
  
  				println("Reading in Flight data frame with OIDs")
  				val flightsDF = sqlContext.read.
  				format("com.objy.spark.sql").
  				option("objy.bootFilePath", AppConfig.Boot).
  				option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
  				option("objy.addOidColumn", "flightOid").
  				load 
  				flightsDF.registerTempTable("flightsTable")
  
  				println("Reading in Airport data frame with OIDs")
  				val airportsDF = sqlContext.read.
  				format("com.objy.spark.sql").
  				option("objy.bootFilePath", AppConfig.Boot).
  				option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Airport").
  				option("objy.addOidColumn", "airportOid").
  				load 
  				airportsDF.registerTempTable("airportsTable")
  
  				println("Reading in Airline data frame with OIDs")
  				val airlinesDF = sqlContext.read.
  				format("com.objy.spark.sql").
  				option("objy.bootFilePath", AppConfig.Boot).
  				option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Airline").
  				option("objy.addOidColumn", "airlineOid").
  				load 
  				airlinesDF.registerTempTable("airlinesTable")
  
  				//			flightsDF.show(10)
  
  				println("Creating relationships between airline and flight via a JOIN")
  				val airlineFlightJoin = """SELECT airlinesTable.airlineOid, airlinesTable.airlineId, flightsTable.airlineId from airlinesTable inner join flightsTable ON airlinesTable.airlineId=flightsTable.airlineId"""
  
  				val flightAirlineDF = sqlContext.sql(airlineFlightJoin)
  
  				//  Write relationships back to the federated database
  				flightAirlineDF.write.
  				mode(SaveMode.Overwrite).
  				format("com.objy.spark.sql").
  				option("objy.bootFilePath", AppConfig.Boot).
  				option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Airline").
  				option("objy.updateByOid", "airlineOid").
  				save() 
  
  				println("Creating relationships between flight and airports via a JOIN")
  				val flightOriginJoin = """SELECT flightsTable.flightOid, flightsTable.origin, flightsTable.destination, airportsTable.IATA from flightsTable inner join airportsTable ON flightsTable.origin=airportsTable.IATA"""
  				val flightDestinationJoin = """SELECT flightsTable.flightOid, flightsTable.origin, flightsTable.destination, airportsTable.IATA from flightsTable inner join airportsTable ON flightsTable.destination=airportsTable.IATA"""
  
  				val flightOriginDF = sqlContext.sql(flightOriginJoin)
  				val flightDestinationnDF = sqlContext.sql(flightDestinationJoin)
  
  				flightOriginDF.write.
  				mode(SaveMode.Overwrite).
  				format("com.objy.spark.sql").
  				option("objy.bootFilePath", AppConfig.Boot).
  				option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
  				option("objy.updateByOid", "flightOid").
  				save() 
  
  
  				flightDestinationnDF.write.
  				mode(SaveMode.Overwrite).
  				format("com.objy.spark.sql").
  				option("objy.bootFilePath", AppConfig.Boot).
  				option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
  				option("objy.updateByOid", "flightOid").
  				save() 
  
  				flightOriginDF.show(10)
  				flightDestinationnDF.show(10)
  
  				var stop = java.lang.System.currentTimeMillis()
  				println("Time to create relationships is: " + (stop-start) + " ms")
  
  			} catch {
  				case ex: Exception =>
  				{
  					println("Exception Thrown - " + ex.getMessage)
  					println(ex.printStackTrace())
  				}
  			}
			} else {
			  
	
  			var start = java.lang.System.currentTimeMillis()
 
  			println("Loading Flights data ...")
  			start = java.lang.System.currentTimeMillis()
  			val flightsCSV = sc.textFile(AppConfig.DataDirectory +"/flights") 
  			val flightsRDD = flightsCSV.map(Flight.flightFromString(_))   
  			val flightsCount = flightsRDD.count
  			
   			var flightsDF = sqlContext.createDataFrame(flightsRDD)
  			println(s"Flights: $flightsCount")
  			println("Flights schema:")
  			flightsDF.printSchema()
  			flightsDF.write.mode(SaveMode.Overwrite).
  			format("com.objy.spark.sql").
  			option("objy.bootFilePath", AppConfig.Boot).
  			option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
  			save()
    		
        var stop = java.lang.System.currentTimeMillis()
  			println("Time to ingest Flights" + (flightsCount) + " items is: " + (stop-start) + " ms")
 
			}
			sc.stop
	}

	def toTimeStamp(format:SimpleDateFormat, dateString:String ):Long = {
			// format 2012/11/11
			val formatDate = format.parse(dateString);
			val miliSecondForDate = formatDate.getTime();
			return miliSecondForDate / 1000;
	}

}