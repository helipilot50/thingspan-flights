package com.objectivity.thingspan.examples.flights.spring

import org.springframework.ui.Model
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.ResponseBody

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}

import com.objectivity.thingspan.examples.flights.Airline
import com.objectivity.thingspan.examples.flights.Flight
import com.objectivity.thingspan.examples.flights.AppConfig


@RestController
@RequestMapping(Array("/flights"))
class GraphController {

	@Autowired
	var sc : SparkContext = null
	var sql : SQLContext = null


	def sqlContext() : SQLContext = {
			if (sql == null)
				sql = new SQLContext(sc);
			sql
	}

	@RequestMapping(Array("/{from}/{to}"))
	@ResponseBody
	def list(@PathVariable("from") low : String, @PathVariable("to") high : String) = {

						val lowDate = Flight.formatDate(low.substring(0,8))
								val lowTime = Flight.padTime(low.substring(8))
								val highDate = Flight.formatDate(high.substring(0,8))
								val highTime = Flight.padTime(high.substring(8))

			var flights: Array[Flight] = null

					if (AppConfig.TestData){
					  // read data from file(s)
						//flights = TestData.aLotOfFlights(lowDate, lowTime, highDate, highTime)
						flights = TestData.scalaFlights(lowDate, lowTime, highDate, highTime)
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
                        | year,
                        | dayOfMonth,
                        | flightDate,
                        | airlineId,
                        | carrier,
                        | flightNumber,
                        | origin,
                        | destination,
                        | departureTime,
                        | arrivalTime,
                        | elapsedTime,
                        | airTime,
                        | distance
        								| FROM flightsTable
				        				| WHERE flightDate >= $lowDate and flightDate <= $highDate and departureTime >= $lowTime and departureTime <= $highTime"""

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
						flights = flightsRDD.collect
					}
	val numOfFlights = flights.length
	println(s"Flights count: $numOfFlights")
	flights.take(5).foreach { println }
	flights
	}


}