package com.objectivity.thingspan.examples.flights

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.SaveMode

import java.text.SimpleDateFormat

class FlightLine(line:Array[String]) extends Serializable {

}

object FlightsLoader {
	def main(args: Array[String]) = {

			val bootFile: String = Constants.Boot // Bootfile for federated database

					println("Loading flights data ...")

					var conf = new SparkConf()
					conf.setAppName("FlightLoader")
					conf.setMaster("local[*]")
					val sc = new SparkContext(conf)
					val sqlContext = new SQLContext(sc);


	val csv = sc.textFile("flights") //data directory or file with CSV flit info
			val flightsRDD = csv.map(_.split(",")).map(p => 
			Flight(p(1).toInt,  // YEAR
					p(2).toInt,     // DAY_OF_MONTH  
					p(3).trim,      // FL_DATE
					p(4).toInt,     // AIRLINE_ID
					p(5).trim,      // CARRIER
					p(6).toInt,     // FL_NUMBER
					p(8).trim,      // ORIGIN
					p(11).trim,     // DESTINATION
					p(14).trim,     // DEP_TIME
					p(15).trim,     // ARR_TIME
					p(16).trim,     // ELAPSED_TIME
					p(17).trim,     // AIR_TIME
					p(18).toInt))   // DISTANCE

			val originAirportsRDD = csv.map(_.split(",")).map(p => 
			Airport(p(8).trim,  // ID
					p(9).trim,      // CITY_NAME        
					p(10).trim)     // STATE
					).distinct()

			val destinationAirportsRDD = csv.map(_.split(",")).map(p => 
			Airport(p(11).trim,  // ID
					p(12).trim,      // CITY_NAME        
					p(13).trim)      // STATE
					).distinct()

			val airlinesRDD = csv.map(_.split(",")).map(p => 
			Airline(p(4).toInt,  // ID
					p(5).trim)      // CARRIER
					).distinct()

			val airportsRDD = originAirportsRDD.union(destinationAirportsRDD).distinct();


	val flightsCount = flightsRDD.count
			val originCount = originAirportsRDD.count
			val destCount = destinationAirportsRDD.count
			val airportsCount = airportsRDD.count
			val airlinesCount = airlinesRDD.count


			var flightsDF = sqlContext.createDataFrame(flightsRDD)
			println(s"Flights: $flightsCount")
			println("Flights schema:")
			flightsDF.printSchema()


			var airportsDF = sqlContext.createDataFrame(airportsRDD)
			println(s"Aitports: $airportsCount")
			println("Airports schema:")
			airportsDF.printSchema()

			var airlinesDF = sqlContext.createDataFrame(airlinesRDD)
			println(s"Airlines: $airlinesCount")
			println("Airlines schema:")
			airlinesDF.printSchema()

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

			flightsDF.write.mode(SaveMode.Overwrite).
			format("com.objy.spark.sql").
			option("objy.bootFilePath", bootFile).
			option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
			save()

			var stop = java.lang.System.currentTimeMillis()
			println("Time to ingest " + flightsCount + " items is: " + (stop-start) + " ms")




			sc.stop
	}

	def toTimeStamp(format:SimpleDateFormat, dateString:String ):Long = {
			// format 2012/11/11
			val formatDate = format.parse(dateString);
			val miliSecondForDate = formatDate.getTime();
			return miliSecondForDate / 1000;
	}

}