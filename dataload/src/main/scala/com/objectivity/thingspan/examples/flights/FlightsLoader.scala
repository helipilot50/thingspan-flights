package com.objectivity.thingspan.examples.flights

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.SaveMode

import java.text.SimpleDateFormat

//class FlightLine(line:Array[String]) extends Serializable {
//
//}

class FlightsLoader {
  
}

object FlightsLoader {
	def main(args: Array[String]) = {

			val bootFile: String = AppConfig.Boot // Bootfile for federated database

					println("Loading flights data ...")

					var conf = new SparkConf()
					conf.setAppName("FlightLoader")
					conf.setMaster("local[1]")

					// Turn off extra info for serializer exceptions (not working)
					conf.set("spark.serializer.extraDebugInfo", "false")

					val sc = new SparkContext(conf)
					val sqlContext = new SQLContext(sc);
	import sqlContext.implicits._

	val flightsCSV = sc.textFile(AppConfig.DataDirectory +"/flights") 
	val flightsRDD = flightsCSV.map(Flight.flightFromString(_))   

	val airportsCSV = sc.textFile(AppConfig.DataDirectory +"/airports")
	val airportsRDD = airportsCSV.map(Airport.airportFromString(_))

	val airlinesCSV = sc.textFile(AppConfig.DataDirectory +"/airlines")
	val airlinesRDD = airlinesCSV.map(Airline.airlineFromString(_))

	val routesCSV = sc.textFile(AppConfig.DataDirectory +"/routes")
	val routesRDD = routesCSV.map(Route.routeFromString(_))

	val flightsCount = flightsRDD.count
	val airportsCount = airportsRDD.count
	val airlinesCount = airlinesRDD.count
	val routesCount = routesRDD.count


	var flightsDF = sqlContext.createDataFrame(flightsRDD)
	println(s"Flights: $flightsCount")
	println("Flights schema:")
	flightsDF.printSchema()


	var airportsDF = sqlContext.createDataFrame(airportsRDD)
	println(s"Airports: $airportsCount")
	println("Airports schema:")
	airportsDF.printSchema()

	var airlinesDF = sqlContext.createDataFrame(airlinesRDD)
	println(s"Airlines: $airlinesCount")
	println("Airlines schema:")
	airlinesDF.printSchema()

	var routesDF = sqlContext.createDataFrame(routesRDD)
	println(s"Routes: $routesCount")
	println("Routes schema:")
	routesDF.printSchema()

	var start = java.lang.System.currentTimeMillis()

	airlinesDF.write.mode(SaveMode.Overwrite).
	format("com.objy.spark.sql").
	option("objy.bootFilePath", bootFile).
	option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Airline").
	save()

	airportsDF.write.mode(SaveMode.Overwrite).
	format("com.objy.spark.sql").
	option("objy.bootFilePath", bootFile).
	option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Airport").
	save()

	routesDF.write.mode(SaveMode.Overwrite).
	format("com.objy.spark.sql").
	option("objy.bootFilePath", bootFile).
	option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Routes").
	save()

	flightsDF.write.mode(SaveMode.Overwrite).
	format("com.objy.spark.sql").
	option("objy.bootFilePath", bootFile).
	option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
	save()

	var stop = java.lang.System.currentTimeMillis()
	println("Time to ingest " + (flightsCount+airlinesCount+airportsCount+routesCount) + " items is: " + (stop-start) + " ms")

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