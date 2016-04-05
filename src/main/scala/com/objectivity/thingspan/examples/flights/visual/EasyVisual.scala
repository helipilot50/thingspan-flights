package com.objectivity.thingspan.examples.flights.visual

import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.graphstream.graph.implementations._
import org.graphstream.graph.Node
import com.objectivity.thingspan.examples.flights.model.TestData
import com.objectivity.thingspan.examples.flights.model.Flight
import com.objectivity.thingspan.examples.flights.model.Airport
import com.objectivity.thingspan.examples.flights.model.AppConfig


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

			val graph = new SingleGraph("Flights")
			graph.addAttribute("ui.stylesheet","url(file:.//style/stylesheet)") 
			graph.addAttribute("ui.quality") 
			graph.addAttribute("ui.antialias")
			graph.setStrict(false)
			graph.setAutoCreate(true)

			val airports = vis.listAirports("United States")
			val airportsCount = airports.count
			airports.collect.foreach { airport => 
				  val node = graph.addNode(airport.IATA).asInstanceOf[SingleNode] 
				  node.addAttribute("name", airport.IATA)
				  node.addAttribute("ui.label", airport.IATA)
				  node.addAttribute("ui.color", 1: java.lang.Double)
			    }
			
			val flights = vis.listFlights("201201230000", "201201240900")
			val flightsCount = flights.count()

			println(s"Airports count: $airportsCount")
			println(s"Flights count: $flightsCount")
			
			flights.collect.foreach { flight =>
			  val fn = flight.carrier + flight.flightNumber
				val edge = graph.addEdge(flight.flightDate + flight.origin+fn, flight.origin, flight.destination, 
						true).
						asInstanceOf[AbstractEdge] 
				
//				edge.addAttribute("name", fn)
//				edge.addAttribute("ui.label", fn)
//				edge.addAttribute("ui.color", 0: java.lang.Double)
			}

			println("before count: " + graph.getNodeCount())
			
			var it = graph.getEachNode().iterator()
			while (it.hasNext()){
			  var node = it.next().asInstanceOf[Node]
			  val degree = node.getDegree()
			 // println(degree)
			  if (node.getDegree() == 0 ){
			    graph.removeNode(node.getId)
			  }
			}

			println("after count: " + graph.getNodeCount())
			
			graph.display()
			sc.stop
	}

}


class EasyVisual(sc : SparkContext, sqlContext : SQLContext) {
  
  
  def listAirports(country: String) = {
   	var airports: RDD[Airport] = null

		if (AppConfig.TestData){
 			airports = TestData.airports(sc, "United States")
		} else {
		  
		  val airportsDF = sqlContext.read.
					format("com.objy.spark.sql").
					option("objy.bootFilePath", AppConfig.Boot).
					option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Airport").
					option("objy.addOidColumn", "airportOid").
					load 

			airportsDF.registerTempTable("airportssTable")

		  val airportsQuery = s"""SELECT
                airportOid,
								name,
								city,
								country,
								IATA,
								ICAO,
								latitude,
								longitude,
								altitude,
								timezone,
								DST,
								tz
								WHERE country = '$country' """

		  val airportsForCriteria = sqlContext.sql(airportsQuery)

					val airportsRDD = airportsForCriteria.map {
								case Row(airportId: Int,
										name: String,
										city: String,
										country: String,
										iATA: String,
										iCAO: String,
										latitude: Double,
										longitude: Double,
										altitude: Int,
										timezone: Double,
										dST: String,
										tz: String) => Airport(airportId,
												name,
												city,
												country,
												iATA,
												iCAO,
												latitude,
												longitude,
												altitude,
												timezone,
												dST,
												tz)
						}
						airports = airportsRDD
					}
					airports.filter { ad => !ad.IATA.isEmpty() }
		}
 		  
	


	def listFlights(low : String, high : String) = {

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