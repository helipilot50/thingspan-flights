package com.objectivity.thingspan.examples.flights

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkFlightsAnalytics {
  def main(args:Array[String]) = {
  	val sc = new SparkContext(new SparkConf().setAppName("FlightAnalytics").setMaster("local[*]"))
		val csv = sc.textFile("data")
		val data = csv.map(line => line.split(",").map(elem => elem.trim))
		val totalData = data.count()
		val startDate = "2012/01/15"
		val start = java.lang.System.currentTimeMillis()
		val flights = data.filter(flight => flight(3) == startDate)
		val totalFlights = flights.count()
		val lateFlights = flights.filter(flight => toMinutes(flight(16)) > (toMinutes(flight(15)) - toMinutes(flight(14))))
		val totalLateFlights = lateFlights.count()
		val lateAirlines = lateFlights.map(xx => (xx(5), 1)).reduceByKey(_+_)
		lateAirlines.collect().foreach(println)
		val stop = java.lang.System.currentTimeMillis()
		println("Time to analyze late flights is: " + (stop-start) + " ms")
		println("Data count: " + totalData)
		println("Flights on " + startDate + " is " + totalFlights)
		println("Late flights on " + startDate + " is " + totalLateFlights)
  }
	def toMinutes(timeString:String):Long = {
	  val time = timeString.toLong
	  val hours = time / 100 
	  val minutes = time % 100
	  hours * 60 + minutes
	}
}