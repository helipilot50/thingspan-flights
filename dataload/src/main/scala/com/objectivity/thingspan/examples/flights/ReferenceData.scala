package com.objectivity.thingspan.examples.flights

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.SaveMode
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser


class ReferenceData{
  
}

object ReferenceData {
 	def main(args: Array[String]) = {
			var options = new Options();
			options.addOption("d", "data", true, "Data directory");
			options.addOption("b", "boot", true, "Boot file");
			options.addOption("u", "usage", false, "Print usage.");

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

			
			val bootFile: String = AppConfig.Boot // Bootfile for federated database

			

			var conf = new SparkConf()
			conf.setAppName("ReferenceDataLoader")
			

			// Turn off extra info for serializer exceptions (not working)
			conf.set("spark.serializer.extraDebugInfo", "false")

			val sc = new SparkContext(conf)
			val sqlContext = new SQLContext(sc);
			import sqlContext.implicits._

			  
			  println("Loading reference data ...")
 
  			var start = java.lang.System.currentTimeMillis()
   			val airportsCSV = sc.textFile(AppConfig.DataDirectory +"/airports")
  			val airportsRDD = airportsCSV.map(Airport.airportFromString(_))
  
  			val airlinesCSV = sc.textFile(AppConfig.DataDirectory +"/airlines")
  			val airlinesRDD = airlinesCSV.map(Airline.airlineFromString(_))
  
  			val routesCSV = sc.textFile(AppConfig.DataDirectory +"/routes")
  			val routesRDD = routesCSV.map(Route.routeFromString(_))
  			
  			val airportsCount = airportsRDD.count
  			val airlinesCount = airlinesRDD.count
  			val routesCount = routesRDD.count
 
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
  			
   			var stop = java.lang.System.currentTimeMillis()
  			println("Time to ingest Airlines/Airports/Routes" + (airlinesCount+airportsCount+routesCount) + " items is: " + (stop-start) + " ms")
 
			sc.stop
	}

 
}