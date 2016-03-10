package com.objectivity.thingspan.examples.flights

import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType

object CreateRelationships {

	def main(args: Array[String]) {

		// Create Spark Configuration
		val sparkConf = new SparkConf()
				.setAppName("CreateRelationships")
				.setMaster("local[1]") // set to 1 to prevent Lock issues

				// Turn off extra info for serializer exceptions (not working)
				sparkConf.set("spark.serializer.extraDebugInfo", "false")

				// Create Spark Context
				val sparkContext = new SparkContext(sparkConf)

				try {
					// Create SQL Context
					println("Creating SQL context")
					val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
					import sqlContext.implicits._
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
					//			airlinesDF.show(10)
					//			airportsDF.show(10)


					println("Creating relationships between airline and flight via a JOIN")
					val airlineFlightJoin = """SELECT airlinesTable.airlineOid, airlinesTable.id, flightsTable.airlineId from airlinesTable inner join flightsTable ON airlinesTable.id=flightsTable.airlineId"""

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
					val flightOriginJoin = """SELECT flightsTable.flightOid, flightsTable.origin, flightsTable.destination, airportsTable.id from flightsTable inner join airportsTable ON flightsTable.origin=airportsTable.id"""
					val flightDestinationJoin = """SELECT flightsTable.flightOid, flightsTable.origin, flightsTable.destination, airportsTable.id from flightsTable inner join airportsTable ON flightsTable.destination=airportsTable.id"""

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
					return ;
				}
				}
				//  Stop Spark Context
				println("Stop Spark Context")
				sparkContext.stop
	}
}


