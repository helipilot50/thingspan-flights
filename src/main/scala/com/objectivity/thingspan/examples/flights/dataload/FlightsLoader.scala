package com.objectivity.thingspan.examples.flights.dataload

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser
import java.text.SimpleDateFormat
import com.objectivity.thingspan.examples.flights.model.AppConfig
import com.objectivity.thingspan.examples.flights.model.Flight
import scala.reflect.runtime.universe


class FlightsLoader {

}

object FlightsLoader {
	def load() = {

			var conf = new SparkConf()
					conf.setAppName("FlightDataLoader")

					// Turn off extra info for serializer exceptions (not working)
					conf.set("spark.serializer.extraDebugInfo", "false")

					val sc = new SparkContext(conf)
					val sqlContext = new SQLContext(sc);
			import sqlContext.implicits._

			try {
				var start = java.lang.System.currentTimeMillis()

						println("Loading Flights data ...")
						start = java.lang.System.currentTimeMillis()
						val flightsCSV = sc.textFile(AppConfig.DataDirectory +"/flights") 
						val flightsRDD = flightsCSV.map(Flight.flightFromCSV(_))   
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


						println("Creating relationships")

						start = java.lang.System.currentTimeMillis()

						//  				println("Reading in Flight data frame with OIDs")
						//  				val flightsDF = sqlContext.read.
						//  				format("com.objy.spark.sql").
						//  				option("objy.bootFilePath", AppConfig.Boot).
						//  				option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
						//  				option("objy.addOidColumn", "flightOid").
						//  				load 
						//  				flightsDF.registerTempTable("flightsTable")

						println("Reading in Airport data frame with OIDs")
						val airportsDF = sqlContext.read.
						format("com.objy.spark.sql").
						option("objy.bootFilePath", AppConfig.Boot).
						option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Airport").
						option("objy.addOidColumn", "airportOid").
						load 
						airportsDF.registerTempTable("airportsTable")

						//  				println("Reading in Airline data frame with OIDs")
						//  				val airlinesDF = sqlContext.read.
						//  				format("com.objy.spark.sql").
						//  				option("objy.bootFilePath", AppConfig.Boot).
						//  				option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Airline").
						//  				option("objy.addOidColumn", "airlineOid").
						//  				load 
						//  				airlinesDF.registerTempTable("airlinesTable")


						//  				println("Creating relationships between airline and flight via a JOIN")
						//  				val airlineFlightJoin = """SELECT airlinesTable.airlineOid, airlinesTable.airlineId, flightsTable.airlineId from airlinesTable inner join flightsTable ON airlinesTable.airlineId=flightsTable.airlineId"""
						//  
						//  				val flightAirlineDF = sqlContext.sql(airlineFlightJoin)
						//  
						//  				flightAirlineDF.write.
						//  				mode(SaveMode.Overwrite).
						//  				format("com.objy.spark.sql").
						//  				option("objy.bootFilePath", AppConfig.Boot).
						//  				option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Airline").
						//  				option("objy.updateByOid", "airlineOid").
						//  				save() 

						println("Creating relationships between flight and airports via a JOIN")
						val flightOriginJoin = """SELECT flightsTable.flightOid, flightsTable.origin, flightsTable.destination, airportsTable.IATA from flightsTable inner join airportsTable ON flightsTable.origin=airportsTable.IATA"""
						//  				val flightDestinationJoin = """SELECT flightsTable.flightOid, flightsTable.origin, flightsTable.destination, airportsTable.IATA from flightsTable inner join airportsTable ON flightsTable.destination=airportsTable.IATA"""

						val flightOriginDF = sqlContext.sql(flightOriginJoin)
						flightOriginDF.show(10)
						flightOriginDF.printSchema()

						flightOriginDF.write.
						mode(SaveMode.Overwrite).
						format("com.objy.spark.sql").
						option("objy.bootFilePath", AppConfig.Boot).
						option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
						option("objy.updateByOid", "flightOid").
						save() 



						//  				val flightDestinationnDF = sqlContext.sql(flightDestinationJoin)
						//  				flightDestinationnDF.show(10)


						//  				flightDestinationnDF.write.
						//  				mode(SaveMode.Overwrite).
						//  				format("com.objy.spark.sql").
						//  				option("objy.bootFilePath", AppConfig.Boot).
						//  				option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
						//  				option("objy.updateByOid", "flightOid").
						//  				save() 


						stop = java.lang.System.currentTimeMillis()
						println("Time to create relationships is: " + (stop-start) + " ms")

			} catch {
  			case ex: Exception =>
  			{
  				println("Exception Thrown - " + ex.getMessage)
  				println(ex.printStackTrace())
  			}
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