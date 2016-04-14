package com.objectivity.thingspan.examples.flights.dataload

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.objectivity.thingspan.examples.flights.model.Flight
import com.objectivity.thingspan.examples.flights.model.AppConfig
import com.objectivity.thingspan.examples.flights.model.Route
import com.objectivity.thingspan.examples.flights.model.Airline
import com.objectivity.thingspan.examples.flights.model.Airport

object Resequence {
  def main(args: Array[String]) {
  	val sc = new SparkContext(new SparkConf().setAppName("Flight Resequence").setMaster(AppConfig.SparkMaster))
  	val sqlContext = new SQLContext(sc)
		val start = java.lang.System.currentTimeMillis()
		
		// airports
		val airportsCSV = sc.textFile(AppConfig.DataDirectory +"/airports/csv")
  	val airportsRDD = airportsCSV.map(Airport.airportFromCSV(_))
    var airportsDF = sqlContext.createDataFrame(airportsRDD).orderBy("country", "city")
    airportsDF.toJSON.saveAsTextFile(AppConfig.DataDirectory +"/airports/json")

  	// airlines
    val airlinesCSV = sc.textFile(AppConfig.DataDirectory +"/airlines/csv")
  	val airlinesRDD = airlinesCSV.map(Airline.airlineFromCSV(_))
  	var airlinesDF = sqlContext.createDataFrame(airlinesRDD)
  	airlinesDF.toJSON.saveAsTextFile(AppConfig.DataDirectory +"/airlines/json")
  	
  	// routes
  	val routesCSV = sc.textFile(AppConfig.DataDirectory +"/routes/csv")
  	val routesRDD = routesCSV.map(Route.routeFromCSV(_))
  	var routesDF = sqlContext.createDataFrame(routesRDD)
  	routesDF.toJSON.saveAsTextFile(AppConfig.DataDirectory +"/routes/json")
  	
  	// flights
		val csv = sc.textFile(AppConfig.DataDirectory +"/flights/csv")
		val flights = csv.map(Flight.flightFromCSV(_))
		val flightsDF = sqlContext.createDataFrame(flights)
		val flightsOrderedDF = flightsDF.orderBy("flightDate", "departureTime")
    flightsOrderedDF.toJSON.saveAsTextFile("data/flights/json")
		val stop = java.lang.System.currentTimeMillis()
		println("Time to reformat data is: " + (stop-start) + " ms")
  }
}