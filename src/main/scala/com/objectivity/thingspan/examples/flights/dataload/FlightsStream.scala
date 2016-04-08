package com.objectivity.thingspan.examples.flights.dataload
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser
import org.apache.spark.sql.functions
import com.objectivity.thingspan.examples.flights.model.AppConfig
import com.objectivity.thingspan.examples.flights.model.Flight
import scala.reflect.runtime.universe

object FlightsStream {

	def main(args: Array[String]) {
		var options = new Options();
		options.addOption("b", "boot", true, "Boot file");
		options.addOption("h", "host", true, "Host name");
		options.addOption("p", "port", true, "Port");
		options.addOption("t", "time", true, "Time");
		options.addOption("m", "master", true, "Master URL");
		options.addOption("d", "data", true, "Data directory");

		val parser = new PosixParser()
		val cl = parser.parse(options, args, false)



		if (cl.hasOption("b")){
			val bootString = cl.getOptionValue("b", "data/flights.boot")
					AppConfig.Boot = bootString
		}	
		
		if (cl.hasOption("d")){
			val dataDirString = cl.getOptionValue("d", "data")
						AppConfig.DataDirectory = dataDirString
		}	

		
		if (cl.hasOption("h")){
			AppConfig.host = cl.getOptionValue("h", "localhost")
		}	
		
		if (cl.hasOption("p")){
			AppConfig.port = cl.getOptionValue("p", "7777").toInt
		}	

		if (cl.hasOption("t")){
			AppConfig.time = cl.getOptionValue("t", "30").toInt
		}	

	  val sparkConf = new SparkConf().setAppName("FlightsStream")
	  if (cl.hasOption("m")){
			sparkConf.setMaster(cl.getOptionValue("m", "local[*]"))
		}	

		val sc = new SparkContext(sparkConf)

		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._

		val streamingContext = new StreamingContext(sc, Seconds(AppConfig.time))
		
		val flightsCSVDStream = streamingContext.textFileStream("file://"+ AppConfig.DataDirectory +"/flights/csv")

		val flightsDStream = flightsCSVDStream.map(Flight.flightFromCSV(_))
		
		val windowDStream = flightsDStream.window(Seconds(AppConfig.time), Seconds(AppConfig.time * 2))

		windowDStream.foreachRDD(flightsGroup => {
		  val flightsGroupCount = flightsGroup.count() 
			if (flightsGroupCount == 0) {
				println("No fights received in this time interval")
			} else {
			  println(s"Saving $flightsGroupCount to Thingspan")
			  var start = java.lang.System.currentTimeMillis()
				val flightsDF = flightsGroup.toDF()
  			println("Flights schema:")
  			flightsDF.printSchema()
  			flightsDF.write.mode(SaveMode.Overwrite).
  			format("com.objy.spark.sql").
  			option("objy.bootFilePath", AppConfig.Boot).
  			option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
  			save()
  			flightsDF.registerTempTable("flightsTable")
  			
  			// Flight to Airport Associations
  			
  			// Read Airports in the USA
  			val airportsDF = sqlContext.read.
  				format("com.objy.spark.sql").
  				option("objy.bootFilePath", AppConfig.Boot).
  				option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Airport").
  				option("objy.addOidColumn", "airportOid").
  				load.filter($"country"==="United States")
  			airportsDF.registerTempTable("airportsTable")

  			
  			val flightOriginJoin = """SELECT flightsTable.flightOid, flightsTable.origin, flightsTable.destination, airportsTable.IATA from flightsTable inner join airportsTable ON flightsTable.origin=airportsTable.IATA"""
  			val flightDestinationJoin = """SELECT flightsTable.flightOid, flightsTable.origin, flightsTable.destination, airportsTable.IATA from flightsTable inner join airportsTable ON flightsTable.destination=airportsTable.IATA"""
  
  			val flightOriginDF = sqlContext.sql(flightOriginJoin)
  			val flightDestinationnDF = sqlContext.sql(flightDestinationJoin)

  			var repartitionedVolume = flightOriginDF.withColumn("cont", functions.shiftRight(flightOriginDF("flightOid"), 32)).repartition(10, $"cont")
  			
  			repartitionedVolume.write.
  				mode(SaveMode.Overwrite).
  				format("com.objy.spark.sql").
  				option("objy.bootFilePath", AppConfig.Boot).
  				option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
  				option("objy.updateByOid", "flightOid").
  				save() 
  
  			repartitionedVolume = flightDestinationnDF.withColumn("cont", functions.shiftRight(flightDestinationnDF("flightOid"), 32)).repartition(10, $"cont")
  
  			repartitionedVolume.write.
  				mode(SaveMode.Overwrite).
  				format("com.objy.spark.sql").
  				option("objy.bootFilePath", AppConfig.Boot).
  				option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
  				option("objy.updateByOid", "flightOid").
  				save() 
  			
//  			// set Volume to bundle associations.
//       val volumeUpdateRdd = bundleRdd.map(e => (e.volume.volumeOid, e.bundleOid))
//               .groupByKey().map(e => (e._1, e._2.toArray))
//               .toDF("volumeOid", "bundles")
//       
//       volumeUpdateRdd.show(10)
//    
//       val repartitionedVolume = volumeUpdateRdd.withColumn("cont", functions.shiftRight(volumeUpdateRdd("volumeOid"), 32)).repartition(10, $"cont")
//    
//       repartitionedVolume.show(10)
//    
//       repartitionedVolume.write.
//       mode(SaveMode.Overwrite).
//       format("com.objy.spark.sql").
//       option("objy.bootFilePath",  bootPath.value.toString()).
//       option("objy.dataClassName", "objy.test.ObjySeismicVolume").
//       option("objy.updateByOid", "volumeOid").
//       save()      
       
        var stop = java.lang.System.currentTimeMillis()
        var totalTime = (stop-start)
				println(s"$flightsGroupCount saved in $totalTime ms")
			}
		})

		streamingContext.start()
		streamingContext.awaitTermination()
	}
}