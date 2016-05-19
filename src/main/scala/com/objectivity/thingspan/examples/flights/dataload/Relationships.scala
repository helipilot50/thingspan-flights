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
import com.objectivity.thingspan.examples.flights.model.Airport
import com.objectivity.thingspan.examples.flights.model.Airline


class Relationships {

}

object Relationships {
	def load() = {

			var conf = new SparkConf()
					conf.setAppName("FlightDataRelationships").setMaster(AppConfig.SparkMaster)

					// Turn off extra info for serializer exceptions (not working)
					conf.set("spark.serializer.extraDebugInfo", "false")

					val sc = new SparkContext(conf)
					val sqlContext = new SQLContext(sc);
			import sqlContext.implicits._
			
			/*
			 * Start ThingSpan
			 */
			com.objy.db.Objy.startup();

				var start = java.lang.System.currentTimeMillis()

				/*
				 * Create DataFrames from objects stored in ThingSpan
				 */
				println("Reading in Flight data frame with OIDs")
				var flightsDF = sqlContext.read.
				format("com.objy.spark.sql").
				option("objy.bootFilePath", AppConfig.Boot).
				option("objy.dataClassName", Flight.FLIGHT_CLASS_NAME).
				option("objy.addOidColumn", "flightOid").
				load 
				flightsDF.registerTempTable("flightsTable")

				println("Reading in Airport data frame with OIDs")
				val airportsDF = sqlContext.read.
				format("com.objy.spark.sql").
				option("objy.bootFilePath", AppConfig.Boot).
				option("objy.dataClassName", Airport.AIRPORT_CLASS_NAME).
				option("objy.addOidColumn", "airportOid").
				load 
				airportsDF.registerTempTable("airportsTable")

				println("Reading in Airline data frame with OIDs")
				val airlinesDF = sqlContext.read.
				format("com.objy.spark.sql").
				option("objy.bootFilePath", AppConfig.Boot).
				option("objy.dataClassName", Airline.AIRLINE_CLASS_NAME).
				option("objy.addOidColumn", "airlineOid").
				load 
				airportsDF.registerTempTable("airlinesTable")

				println("\n***\n***\n*** Creating relationships between flight and airports via a JOIN\n***\n***\n")

				/*
				 * Spark SQL for origin airport and flight
				 */
				val flightOriginJoin = new StringBuilder()
					flightOriginJoin.append("SELECT airportsTable.airportOid as originAirport, flightsTable.flightOid "); 
  				flightOriginJoin.append("from flightsTable inner join airportsTable ");    
  				flightOriginJoin.append("ON flightsTable.origin=airportsTable.IATA");
  			/*
  			 * Execute the SQL to create ad 'join' DataFrame
  			 * 
  			 * The origin Airport Oid is being added to the flight to create the relationship  
  			 */
				val flightOriginDF = sqlContext.sql(flightOriginJoin.toString())

					flightOriginDF.printSchema()

					flightOriginDF.show(10)
					println("*** Start writing origins to ThingSpan")
					flightOriginDF.write.
						mode(SaveMode.Overwrite).
  						format("com.objy.spark.sql").
  						option("objy.bootFilePath", AppConfig.Boot).
  						option("objy.dataClassName", Flight.FLIGHT_CLASS_NAME).
  						option("objy.updateByOid", "flightOid").
  						save() 
					println("*** Finished writing origins to ThingSpan")
					
				/*
				 * Spark SQL for the flight and origin airport
				 * 
				 * This is the inverse of the above operation and 
				 * it creates an edge from the flight to the origin Airport
				 */
				val flightFromJoin = new StringBuilder()
					flightFromJoin.append("SELECT airportsTable.airportOid, flightsTable.flightOid as outboundFlights"); 
  				flightFromJoin.append("from airportsTable inner join  flightsTable");    
  				flightFromJoin.append("ON flightsTable.origin=airportsTable.IATA");
  
  			/*
  			 * Execute the SQL to create ad 'join' DataFrame
  			 * 
  			 * The Flight Oid is being added to the origin Airport to create the relationship  
  			 */
				val flightFromDF = sqlContext.sql(flightFromJoin.toString())

					flightFromDF.printSchema()

					flightFromDF.show(10)
					println("*** Start writing 'flights from' ThingSpan")
					flightFromDF.write.
						mode(SaveMode.Overwrite).
  						format("com.objy.spark.sql").
  						option("objy.bootFilePath", AppConfig.Boot).
  						option("objy.dataClassName", Airport.AIRPORT_CLASS_NAME).
  						option("objy.updateByOid", "airportOid").
  						save() 
					println("*** Finished writing 'flights from' to ThingSpan")

					val flightDestinationJoin = new StringBuilder()
					flightDestinationJoin.append("SELECT airportsTable.airportOid as destinationAirport, flightsTable.flightOid "); 
  				flightDestinationJoin.append("from flightsTable inner join airportsTable ");    
  				flightDestinationJoin.append("ON flightsTable.destination=airportsTable.IATA");

				val flightDestinationDF = sqlContext.sql(flightDestinationJoin.toString())

						flightDestinationDF.printSchema()

						flightDestinationDF.show(5)
				println("*** Start writing destinations to ThingSpan")
				flightDestinationDF.write.
						mode(SaveMode.Overwrite).
  					format("com.objy.spark.sql").
  						option("objy.bootFilePath", AppConfig.Boot).
  						option("objy.dataClassName", Flight.FLIGHT_CLASS_NAME).
  						option("objy.updateByOid", "flightOid").
  						save() 
				println("*** Finished writing destinations to ThingSpan")

				val flightToJoin = new StringBuilder()
					flightToJoin.append("SELECT airportsTable.airportOid, flightsTable.flightOid as outboundFlights"); 
  				flightToJoin.append("from airportsTable inner join  flightsTable");    
  				flightToJoin.append("ON flightsTable.destination=airportsTable.IATA");
  
				val flightToDF = sqlContext.sql(flightToJoin.toString())

					flightToDF.printSchema()

					flightToDF.show(10)
					println("*** Start writing 'flights to' ThingSpan")
					flightToDF.write.
						mode(SaveMode.Overwrite).
  						format("com.objy.spark.sql").
  						option("objy.bootFilePath", AppConfig.Boot).
  						option("objy.dataClassName", Airport.AIRPORT_CLASS_NAME).
  						option("objy.updateByOid", "flightOid").
  						save() 
					println("*** Finished writing 'flights to' to ThingSpan")

				val stop = java.lang.System.currentTimeMillis()
				println("\n***\n***\n*** Time to create relationships is: " + (stop-start) + " ms\n***\n***\n")

				/*
				 * read some data to make sure it is correct
				 */
				val airportsResultsDF = sqlContext.read.
					format("com.objy.spark.sql").
					option("objy.bootFilePath", AppConfig.Boot).
					option("objy.dataClassName", Airport.AIRPORT_CLASS_NAME).
					option("objy.addOidColumn", "airportOid").
					load 
	      airportsResultsDF.printSchema()

				airportsResultsDF.show(5)
				
			sc.stop
	}

}