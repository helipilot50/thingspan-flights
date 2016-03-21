package com.objectivity.thingspan.examples.flights.visual

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import com.objectivity.thingspan.examples.flights.Airline
import com.objectivity.thingspan.examples.flights.Flight
import com.objectivity.thingspan.examples.flights.AppConfig
import org.graphstream.graph.{Graph => GraphStream}
import org.graphstream.graph.{Graph=>GraphStream}
import org.graphstream.graph.implementations._ 
import org.apache.spark.rdd.RDD
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser


object EasyVisual {

	def main(args: Array[String]) = {
			var options = new Options();
			options.addOption("d", "data", true, "Data directory");
			options.addOption("b", "boot", true, "Boot file");

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
			AppConfig.TestData = true
			AppConfig.DataDirectory = "../data"
				var conf = new SparkConf()
				conf.setAppName("EasyVisual")
				conf.set("spark.serializer.extraDebugInfo", "false")
				conf.setMaster("local")
				val sc = new SparkContext(conf)
				val sqlContext = new SQLContext(sc);
			import sqlContext.implicits._

			var vis = new EasyVisual(sc, sqlContext);
			val flights = vis.list("201201230000", "201201242359")

			val graph: SingleGraph = new SingleGraph("Flights")
			graph.addAttribute("ui.stylesheet","url(file:.//style/stylesheet)") 
			graph.addAttribute("ui.quality") 
			graph.addAttribute("ui.antialias")
			graph.setStrict(false)
			graph.setAutoCreate(true)

//			var airportsRDD = flights.flatMap { flight => Array(flight.origin, flight.destination) }
//			airportsRDD = airportsRDD.distinct()
//			
//			airportsRDD.collect.foreach { airport => 
//				  graph.addNode(airport).asInstanceOf[SingleNode] 
//			    }
			
			flights.collect.foreach { flight =>
				val edge = graph.addEdge(flight.flightDate + flight.origin+flight.carrier + flight.flightNumber, flight.origin, flight.destination, 
						true).
						asInstanceOf[AbstractEdge] 
			}
			graph.display()
			sc.stop
	}

}


class EasyVisual(sc : SparkContext, sqlContext : SQLContext) {


	def list(low : String, high : String) = {

			val lowDate = Flight.formatDate(low.substring(0,8))
					val lowTime = Flight.padTime(low.substring(8))
					val highDate = Flight.formatDate(high.substring(0,8))
					val highTime = Flight.padTime(high.substring(8))

					var flights: RDD[Flight] = null

					if (AppConfig.TestData){
						// read data from file(s)
						//flights = TestData.someFlights(sc)
						//flights = TestData.scalaFlights(sc, lowDate, lowTime, highDate, highTime)
						flights = TestData.aLotOfFlights(sc, lowDate, lowTime, highDate, highTime)
					} else {
						// read flights from Thingspan  
						val flightsDF = sqlContext.read.
								format("com.objy.spark.sql").
								option("objy.bootFilePath", AppConfig.Boot).
								option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
								option("objy.addOidColumn", "flightOid").
								load 

								flightsDF.registerTempTable("flightsTable")

								val flightsQuery = s"""SELECT
								year,
								dayOfMonth,
								flightDate,
								airlineId,
								carrier,
								flightNumber,
								origin,
								destination,
								departureTime,
								arrivalTime,
								elapsedTime,
								airTime,
								distance
								FROM flightsTable
								WHERE (flightDate >= '$lowDate' and departureTime >= '$lowTime') and (flightDate <= '$highDate'  and departureTime <= '$highTime')"""

								val flightForCriteria = sqlContext.sql(flightsQuery)

								val flightsRDD = flightForCriteria.map {
								case Row(year: Int,
										dayOfMonth: Int,
										flightDate: String,
										airlineId: Int,
										carrier: String,
										flightNumber: Int,
										origin: String,
										destination: String,
										departureTime: String,
										arrivalTime: String,
										elapsedTime: Int,
										airTime: Int,
										distance: Int) => Flight(year,
												dayOfMonth,
												flightDate,
												airlineId,
												carrier,
												flightNumber,
												origin,
												destination,
												departureTime,
												arrivalTime,
												elapsedTime,
												airTime,
												distance)
						}
						flights = flightsRDD
					}
					flights
	}
}