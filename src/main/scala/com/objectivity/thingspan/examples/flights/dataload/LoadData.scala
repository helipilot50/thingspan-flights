package com.objectivity.thingspan.examples.flights.dataload

import scala.reflect.runtime.universe
import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import com.objectivity.thingspan.examples.flights.model.AppConfig
import com.objectivity.thingspan.examples.flights.model.Airport
import com.objectivity.thingspan.examples.flights.model.Airline
import com.objectivity.thingspan.examples.flights.model.Route
import com.objectivity.thingspan.examples.flights.model.Flight
import com.objy.db.Connection
import com.objectivity.thingspan.examples.flights.model.Tools
import com.objy.db.Transaction
import com.objy.db.TransactionMode


class LoadData{

}

object LoadData {
  
 	def loadSpark()  {
			
			var conf = new SparkConf().setMaster(AppConfig.SparkMaster)
			conf.setAppName("ReferenceDataLoader")
			

			// Turn off extra info for serializer exceptions (not working)
			conf.set("spark.serializer.extraDebugInfo", "false")

			val sc = new SparkContext(conf)
			val sqlContext = new SQLContext(sc);
			import sqlContext.implicits._

			com.objy.db.Objy.startup();
			val connection = new Connection(AppConfig.Boot)
//		  var tx = new Transaction(TransactionMode.READ_UPDATE)
//		  val routeClass = Tools.fetchRouteClass()
//		  tx.commit()
//		  if (routeClass == null)
//        Tools.registerClasses()
			  
		  println("Loading reference data ...")
 
  	  var start = java.lang.System.currentTimeMillis()
			
			val airlinesCSV = sc.textFile(AppConfig.DataDirectory +"/airlines/csv")
			val airlinesRDD = airlinesCSV.map(Airline.airlineFromCSV(_))
			var airlinesDF = sqlContext.createDataFrame(airlinesRDD)
			val airlinesCount = airlinesDF.count
			println(s"Airlines: $airlinesCount")
			println("Airlines schema:")
			airlinesDF.printSchema()
			airlinesDF.write.mode(SaveMode.Overwrite).
  			format("com.objy.spark.sql").
  			option("objy.bootFilePath", AppConfig.Boot).
  			option("objy.dataClassName", Airline.AIRLINE_CLASS_NAME).
  			save()

			val routesCSV = sc.textFile(AppConfig.DataDirectory +"/routes/csv")
			val routesRDD = routesCSV.map(Route.routeFromCSV(_))
			var routesDF = sqlContext.createDataFrame(routesRDD)
			val routesCount = routesDF.count
		  println(s"Routes: $routesCount")
			println("Routes schema:")
			routesDF.printSchema()
			routesDF.write.mode(SaveMode.Overwrite).
  			format("com.objy.spark.sql").
  			option("objy.bootFilePath", AppConfig.Boot).
  			option("objy.dataClassName", Route.ROUTE_CLASS_NAME).
  			save()
			
   		val airportsCSV = sc.textFile(AppConfig.DataDirectory +"/airports/csv")
			val airportsRDD = airportsCSV.map(Airport.airportFromCSV(_))
			var airportsDF = sqlContext.createDataFrame(airportsRDD)
 			val airportsCount = airportsDF.count
			println(s"Airports: $airportsCount")
			println("Airports schema:")
			airportsDF.printSchema()
			airportsDF.write.mode(SaveMode.Overwrite).
  			format("com.objy.spark.sql").
  			option("objy.bootFilePath", AppConfig.Boot).
  			option("objy.dataClassName", Airport.AIRPORT_CLASS_NAME).
  			save()

  		var stop = java.lang.System.currentTimeMillis()

      println("Time to ingest Airlines/Airports/Routes" + (airlinesCount+/*airportsCount+*/routesCount) + " items is: " + (stop-start) + " ms")

     	println("Loading Flights data ...")
     	
     	start = java.lang.System.currentTimeMillis()
				val flightsCSV = sc.textFile(AppConfig.DataDirectory +"/flights/csv") 
				val flightsRDD = flightsCSV.map(Flight.flightFromCSV(_))   
				val flightsCount = flightsRDD.count

				var flightsDF = sqlContext.createDataFrame(flightsRDD)
				println(s"Flights: $flightsCount")
				println("Flights schema:")
				flightsDF.printSchema()
				flightsDF.write.mode(SaveMode.Overwrite).
					format("com.objy.spark.sql").
					option("objy.bootFilePath", AppConfig.Boot).
					option("objy.dataClassName", Flight.FLIGHT_CLASS_NAME).
					save()
				flightsDF.registerTempTable("flightsTable")
				stop = java.lang.System.currentTimeMillis()
				println("Time to ingest Flights" + (flightsCount) + " items is: " + (stop-start) + " ms")


			sc.stop
	}

 
}