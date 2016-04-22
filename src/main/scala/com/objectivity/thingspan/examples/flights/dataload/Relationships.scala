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


class Relationships {

}

object Relationships {
	def load() = {

			var conf = new SparkConf()
					conf.setAppName("FlightDataLoader").setMaster(AppConfig.SparkMaster)

					// Turn off extra info for serializer exceptions (not working)
					conf.set("spark.serializer.extraDebugInfo", "false")

					val sc = new SparkContext(conf)
					val sqlContext = new SQLContext(sc);
			import sqlContext.implicits._

			try {
				var start = java.lang.System.currentTimeMillis()

						println("Reading in Flight data frame with OIDs")
						var flightsDF = sqlContext.read.
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

						println("\n***\n***\n*** Creating relationships between flight and airports via a JOIN\n***\n***\n")

						/*
						 * Spark SQL for origin airport and flight
						 */
						val flightOriginJoin = new StringBuilder()
						flightOriginJoin.append("SELECT airportOid, flightOid "); 
    				flightOriginJoin.append("from flightsTable inner join airportsTable ");    
    				flightOriginJoin.append("ON flightsTable.origin=airportsTable.IATA");
    
				val flightOriginDF = sqlContext.sql(flightOriginJoin.toString())

						flightOriginDF.printSchema()

						flightOriginDF.show(5)
						println("*** Writing origins to ThingSpan")
						flightOriginDF.write.
  						mode(SaveMode.Overwrite).
    						format("com.objy.spark.sql").
    						option("objy.bootFilePath", AppConfig.Boot).
    						option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
    						option("objy.updateByOid", "flightOid").
    						save() 
						
    				/*
    				 * Spark SQL for destination airport and flight
    				 */
    				val flightDestinationJoin = new StringBuilder()
    						flightDestinationJoin.append("SELECT airportOid, flightOid "); 
    				flightDestinationJoin.append("from flightsTable inner join airportsTable ");    
    				flightDestinationJoin.append("ON flightsTable.destination=airportsTable.IATA");

				val flightDestinationDF = sqlContext.sql(flightDestinationJoin.toString())

						flightDestinationDF.printSchema()

						flightDestinationDF.show(5)
						println("*** Writing destinations to ThingSpan")
						flightDestinationDF.write.
  						mode(SaveMode.Overwrite).
    						format("com.objy.spark.sql").
    						option("objy.bootFilePath", AppConfig.Boot).
    						option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
    						option("objy.updateByOid", "flightOid").
    						save() 

						val stop = java.lang.System.currentTimeMillis()
						println("\n***\n***\n*** Time to create relationships is: " + (stop-start) + " ms\n***\n***\n")

			} catch {
  			case ex: Exception => {
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