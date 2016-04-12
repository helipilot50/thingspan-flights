package com.objectivity.thingspan.examples.flights.visual

import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import com.objectivity.thingspan.examples.flights.model.TestData
import com.objectivity.thingspan.examples.flights.model.Flight
import com.objectivity.thingspan.examples.flights.model.Airport
import com.objectivity.thingspan.examples.flights.model.AppConfig
import org.graphstream.graph.implementations._
import org.graphstream.graph.Node
import org.graphstream.graph.IdAlreadyInUseException
import org.graphstream.graph.ElementNotFoundException
import org.apache.spark.api.java.JavaRDD


object EasyVisual {

	def show() = {
			var conf = new SparkConf()
					conf.setAppName("EasyVisual")
					conf.set("spark.serializer.extraDebugInfo", "false")
					conf.setMaster("local")
					val sc = new SparkContext(conf)
					val sqlContext = new SQLContext(sc);
			import sqlContext.implicits._

			var vis = new EasyVisual(sc, sqlContext);


			// Display window
			GraphQueryWindow.show(vis)

			//val airportsRef = vis.listAirports("United States").map(ap => (ap.IATA, ap))

			//val fltAD = vis.listFlightsBetween("201201230850", "201201230851")
			//val fltAD = vis.listFlightsFrom("DEN", "201201230800", "201201230900")
			//val fltAD = vis.listFlightsTo("DEN", "201201230800", "201201230900")
			//val fltAD = vis.listFlightsBetween("DEN", "SFO", "201201230000", "201201232359")


			//			fltAD._1.collect.foreach { flight =>
			//			val fn = flight.carrier + flight.flightNumber
			//			try {
			//				val edge = graph.addEdge(flight.flightDate + flight.origin+fn, flight.origin, flight.destination, 
			//						true).
			//						asInstanceOf[AbstractEdge] 
			//
			//								edge.addAttribute("name", fn)
			//								edge.addAttribute("ui.label", fn)
			//								edge.addAttribute("ui.color", 0: java.lang.Double)
			//			} catch {
			//			  case e: ElementNotFoundException => {}
			//			  case ex: IdAlreadyInUseException => {}
			//			}
			//		}

	}

}


class EasyVisual(sc : SparkContext, sqlContext : SQLContext) {

	def nodeEdgesFor(from:String, to:String, lowDateTime : String, highDateTime : String) : (JavaRDD[Flight], JavaRDD[Airport]) = {
		var fltAD: (RDD[Flight], RDD[(String, (Int, Airport))]) = null
		
				if (from != null && !from.isEmpty() && (to == null || to.isEmpty())){
					fltAD = listFlightsFrom(from, lowDateTime, highDateTime)
				} else if ((from == null || from.isEmpty()) && (to != null && !to.isEmpty())){
					fltAD = listFlightsTo(to, lowDateTime, highDateTime)
				} else if ((from == null || from.isEmpty()) && (to == null || to.isEmpty())){
					fltAD = listFlightsBetweenTimes(lowDateTime, highDateTime)
				} else if ((from != null && !from.isEmpty()) && (to != null && !to.isEmpty())){
					fltAD = listFlightsBetween(from, to, lowDateTime, highDateTime)
				} 

    			val airports = fltAD._2.values.map(x => x._2)
    			val flights = fltAD._1

    (flights.toJavaRDD(), airports.toJavaRDD())
	}

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

	def listFlightsFrom(from:String, lowDateTime : String, highDateTime : String): (RDD[Flight], RDD[(String, (Int, Airport))]) = {

			val lowDate = Flight.formatDate(lowDateTime.substring(0,8))
					val lowTime = Flight.padTime(lowDateTime.substring(8))
					val highDate = Flight.formatDate(highDateTime.substring(0,8))
					val highTime = Flight.padTime(highDateTime.substring(8))

					if (AppConfig.TestData){
						val flights = TestData.aLotOfFlightsFrom(sc, from, lowDate, lowTime, highDate, highTime)
								return (flights, graphAirports(flights))
					} else {
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
								WHERE (origin = '$from') and (flightDate >= '$lowDate' and departureTime >= '$lowTime') and (flightDate <= '$highDate'  and departureTime <= '$highTime')"""

								return listFlights(flightsQuery)
					}
	}

	def listFlightsTo(to:String, lowDateTime : String, highDateTime : String): (RDD[Flight], RDD[(String, (Int, Airport))]) = {

			val lowDate = Flight.formatDate(lowDateTime.substring(0,8))
					val lowTime = Flight.padTime(lowDateTime.substring(8))
					val highDate = Flight.formatDate(highDateTime.substring(0,8))
					val highTime = Flight.padTime(highDateTime.substring(8))

					if (AppConfig.TestData){
						val flights = TestData.aLotOfFlightsTo(sc, to, lowDate, lowTime, highDate, highTime)
								return (flights, graphAirports(flights))
					} else {
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
								WHERE (destination = '$to') and (flightDate >= '$lowDate' and departureTime >= '$lowTime') and (flightDate <= '$highDate'  and departureTime <= '$highTime')"""

								return listFlights(flightsQuery)
					}
	}

	def listFlightsBetween(from:String, to:String, lowDateTime : String, highDateTime : String): (RDD[Flight], RDD[(String, (Int, Airport))]) = {

			val lowDate = Flight.formatDate(lowDateTime.substring(0,8))
					val lowTime = Flight.padTime(lowDateTime.substring(8))
					val highDate = Flight.formatDate(highDateTime.substring(0,8))
					val highTime = Flight.padTime(highDateTime.substring(8))

					if (AppConfig.TestData){
						val flights = TestData.aLotOfFlightsBween(sc, from, to, lowDate, lowTime, highDate, highTime)
								return (flights, graphAirports(flights))
					} else {
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
								WHERE (from = '$from') and (destination = '$to') and (flightDate >= '$lowDate' and departureTime >= '$lowTime') and (flightDate <= '$highDate'  and departureTime <= '$highTime')"""

								return listFlights(flightsQuery)
					}
	}

	def listFlightsBetweenTimes(lowDateTime : String, highDateTime : String): (RDD[Flight], RDD[(String, (Int, Airport))]) = {

			val lowDate = Flight.formatDate(lowDateTime.substring(0,8))
					val lowTime = Flight.padTime(lowDateTime.substring(8))
					val highDate = Flight.formatDate(highDateTime.substring(0,8))
					val highTime = Flight.padTime(highDateTime.substring(8))


					if (AppConfig.TestData){
						val flights = TestData.aLotOfFlights(sc, lowDate, lowTime, highDate, highTime)
								return (flights, graphAirports(flights))
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

								return listFlights(flightsQuery)
					}
	}

	def listFlights(flightsQuery : String) = {

			// read flights from ThingSpan  
			val flightsDF = sqlContext.read.
					format("com.objy.spark.sql").
					option("objy.bootFilePath", AppConfig.Boot).
					option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
					option("objy.addOidColumn", "flightOid").
					load 

					flightsDF.registerTempTable("flightsTable")

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

			(flightsRDD, graphAirports(flightsRDD))
	}

	def graphAirports(flightsRDD: RDD[Flight]): RDD[(String, (Int, Airport))] = {
			val airports = flightsRDD.flatMap( f => Seq(f.origin, f.destination) ).map ( ad => (ad, 1)).reduceByKey((x,y) => x + y)
					val airportsRef = listAirports("United States").map(ap => (ap.IATA, ap))
					airports.join(airportsRef)
	}
	
	def close() {
	  			sc.stop
	}
}