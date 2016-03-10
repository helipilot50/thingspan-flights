package com.objectivity.thingspan.examples.flights

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.SaveMode

import java.text.SimpleDateFormat

class FlightLine(line:Array[String]) extends Serializable {

}

object FlightsLoader {
	def main(args: Array[String]) = {

			val bootFile: String = AppConfig.Boot // Bootfile for federated database

					println("Loading flights data ...")

					var conf = new SparkConf()
					conf.setAppName("FlightLoader")
					conf.setMaster("local[*]")
					val sc = new SparkContext(conf)
					val sqlContext = new SQLContext(sc);


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




			sc.stop
	}

	def toTimeStamp(format:SimpleDateFormat, dateString:String ):Long = {
			// format 2012/11/11
			val formatDate = format.parse(dateString);
			val miliSecondForDate = formatDate.getTime();
			return miliSecondForDate / 1000;
	}

}