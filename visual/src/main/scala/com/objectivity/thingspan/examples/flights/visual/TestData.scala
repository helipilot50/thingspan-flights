package com.objectivity.thingspan.examples.flights.visual

import com.objectivity.thingspan.examples.flights.Flight
import scala.io.Source
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import com.objectivity.thingspan.examples.flights.AppConfig
import org.apache.spark.{SparkConf, SparkContext}
import scala.beans.BeanProperty
import org.apache.spark.rdd.RDD


object TestData {

//	// Find: ^\d+,(\d+),(\d+),(\d\d\d\d/\d\d/\d\d),(\d+),(\w\w),(\d+),\d+,(\w+?),.+?,.+?,(\w+?),.+?,.+?,(\d+),(\d+),(\d+),(\d+),(\d+)
//	// Replace: Flight($1, $2, "$3", $4, "$5", $6, "$7", "$8", "$9", "$10", "$11", "$12", $13),
	def someFlights(sc : SparkContext) : RDD[Flight] = {
			sc.parallelize(Array(	
					Flight(2012, 1, "2012/11/11", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("855"), Flight.padTime("1142"), Flight.timeStringToMinutes("347"), Flight.timeStringToMinutes("330"), 2475),
					Flight(2012, 2, "2012/01/02", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("921"), Flight.padTime("1210"), Flight.timeStringToMinutes("349"), Flight.timeStringToMinutes("325"), 2475),
					Flight(2012, 3, "2012/01/03", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("931"), Flight.padTime("1224"), Flight.timeStringToMinutes("353"), Flight.timeStringToMinutes("319"), 2475),
					Flight(2012, 4, "2012/01/04", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("904"), Flight.padTime("1151"), Flight.timeStringToMinutes("347"), Flight.timeStringToMinutes("309"), 2475),
					Flight(2012, 5, "2012/01/05", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("858"), Flight.padTime("1142"), Flight.timeStringToMinutes("344"), Flight.timeStringToMinutes("306"), 2475),
					Flight(2012, 6, "2012/01/06", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("911"), Flight.padTime("1151"), Flight.timeStringToMinutes("340"), Flight.timeStringToMinutes("321"), 2475),
					Flight(2012, 7, "2012/01/07", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("902"), Flight.padTime("1203"), Flight.timeStringToMinutes("361"), Flight.timeStringToMinutes("337"), 2475),
					Flight(2012, 8, "2012/01/08", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("855"), Flight.padTime("1129"), Flight.timeStringToMinutes("334"), Flight.timeStringToMinutes("318"), 2475),
					Flight(2012, 9, "2012/01/09", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("858"), Flight.padTime("1127"), Flight.timeStringToMinutes("329"), Flight.timeStringToMinutes("307"), 2475),
					Flight(2012, 10, "2012/01/10", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("852"), Flight.padTime("1134"), Flight.timeStringToMinutes("342"), Flight.timeStringToMinutes("325"), 2475),
					Flight(2012, 11, "2012/01/11", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("853"), Flight.padTime("1152"), Flight.timeStringToMinutes("359"), Flight.timeStringToMinutes("322"), 2475),
					Flight(2012, 12, "2012/01/12", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("902"), Flight.padTime("1208"), Flight.timeStringToMinutes("366"), Flight.timeStringToMinutes("322"), 2475),
					Flight(2012, 13, "2012/01/13", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("853"), Flight.padTime("1133"), Flight.timeStringToMinutes("340"), Flight.timeStringToMinutes("313"), 2475),
					Flight(2012, 14, "2012/01/14", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("902"), Flight.padTime("1149"), Flight.timeStringToMinutes("347"), Flight.timeStringToMinutes("324"), 2475),
					Flight(2012, 15, "2012/01/15", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("902"), Flight.padTime("1218"), Flight.timeStringToMinutes("376"), Flight.timeStringToMinutes("339"), 2475),
					Flight(2012, 16, "2012/01/16", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("854"), Flight.padTime("1208"), Flight.timeStringToMinutes("374"), Flight.timeStringToMinutes("350"), 2475),
					Flight(2012, 17, "2012/01/17", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("854"), Flight.padTime("1226"), Flight.timeStringToMinutes("392"), Flight.timeStringToMinutes("350"), 2475),
					Flight(2012, 18, "2012/01/18", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("900"), Flight.padTime("1154"), Flight.timeStringToMinutes("354"), Flight.timeStringToMinutes("328"), 2475),
					Flight(2012, 19, "2012/01/19", 19805, "AA", 1, "JFK", "LAX", Flight.padTime("855"), Flight.padTime("1157"), Flight.timeStringToMinutes("362"), Flight.timeStringToMinutes("333"), 2475)
					), 5)
	}

	def scalaFlights(sc : SparkContext, lowDate:String, lowTime:String, highDate:String, highTime:String) : RDD[Flight] = {
			var flights : ListBuffer[Flight] = ListBuffer[Flight]();
			val dataFile = AppConfig.DataDirectory +"/flights/xbr"
	    Source.fromFile(dataFile).getLines.foreach { line => {
	      val flight = Flight.flightFromString(line)
				if ((flight.flightDate >= lowDate && flight.departureTime >= lowTime) && (flight.flightDate <= highDate  && flight.departureTime <= highTime))
					flights +=  flight
	    }
	  }
		sc.parallelize(flights.toSeq, 5)

	}

		def aLotOfFlights(sc : SparkContext, lowDate:String, lowTime:String, highDate:String, highTime:String) : RDD[Flight] = {
		  val flightsCSV = sc.textFile(AppConfig.DataDirectory +"/flights") 
			val flightsRDD = flightsCSV.map(Flight.flightFromString(_)).filter(fl => {
				((fl.flightDate >= lowDate && fl.departureTime >= lowTime) 
				    && (fl.flightDate <= highDate  && fl.departureTime <= highTime))}
			)
	  flightsRDD
	}

}