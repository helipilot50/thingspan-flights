package com.objectivity.thingspan.examples.flights.spring

import com.objectivity.thingspan.examples.flights.Flight
import scala.io.Source
import scala.util.matching.Regex

object TestData {


	// Find: ^\d+,(\d+),(\d+),(\d\d\d\d/\d\d/\d\d),(\d+),(\w\w),(\d+),\d+,(\w\w\w),.+?,.+?,(\w\w\w),.+?,.+?,(\d+),(\d+),(\d+),(\d+),(\d+)
	// Replace: Flight($1, $2, "$3", $4, "$5", $6, "$7", "$8", "$9", "$10", "$11", "$12", $13),
	def someFlights() : Array[Flight] = {
			Array(	
					Flight(2012, 1, "2012/11/11", 19805, "AA", 1, "JFK", "LAX", "855", "1142", "347", "330", 2475),
					Flight(2012, 2, "2012/01/02", 19805, "AA", 1, "JFK", "LAX", "921", "1210", "349", "325", 2475),
					Flight(2012, 3, "2012/01/03", 19805, "AA", 1, "JFK", "LAX", "931", "1224", "353", "319", 2475),
					Flight(2012, 4, "2012/01/04", 19805, "AA", 1, "JFK", "LAX", "904", "1151", "347", "309", 2475),
					Flight(2012, 5, "2012/01/05", 19805, "AA", 1, "JFK", "LAX", "858", "1142", "344", "306", 2475),
					Flight(2012, 6, "2012/01/06", 19805, "AA", 1, "JFK", "LAX", "911", "1151", "340", "321", 2475),
					Flight(2012, 7, "2012/01/07", 19805, "AA", 1, "JFK", "LAX", "902", "1203", "361", "337", 2475),
					Flight(2012, 8, "2012/01/08", 19805, "AA", 1, "JFK", "LAX", "855", "1129", "334", "318", 2475),
					Flight(2012, 9, "2012/01/09", 19805, "AA", 1, "JFK", "LAX", "858", "1127", "329", "307", 2475),
					Flight(2012, 10, "2012/01/10", 19805, "AA", 1, "JFK", "LAX", "852", "1134", "342", "325", 2475),
					Flight(2012, 11, "2012/01/11", 19805, "AA", 1, "JFK", "LAX", "853", "1152", "359", "322", 2475),
					Flight(2012, 12, "2012/01/12", 19805, "AA", 1, "JFK", "LAX", "902", "1208", "366", "322", 2475),
					Flight(2012, 13, "2012/01/13", 19805, "AA", 1, "JFK", "LAX", "853", "1133", "340", "313", 2475),
					Flight(2012, 14, "2012/01/14", 19805, "AA", 1, "JFK", "LAX", "902", "1149", "347", "324", 2475),
					Flight(2012, 15, "2012/01/15", 19805, "AA", 1, "JFK", "LAX", "902", "1218", "376", "339", 2475),
					Flight(2012, 16, "2012/01/16", 19805, "AA", 1, "JFK", "LAX", "854", "1208", "374", "350", 2475),
					Flight(2012, 17, "2012/01/17", 19805, "AA", 1, "JFK", "LAX", "854", "1226", "392", "350", 2475),
					Flight(2012, 18, "2012/01/18", 19805, "AA", 1, "JFK", "LAX", "900", "1154", "354", "328", 2475),
					Flight(2012, 19, "2012/01/19", 19805, "AA", 1, "JFK", "LAX", "855", "1157", "362", "333", 2475)
					)
	}
	
	def aLotOfFlights() : Array[Flight] = {
	  val Pattern = """^\d+,(\d+),(\d+),(\d\d\d\d/\d\d/\d\d),(\d+),(\w\w),(\d+),\d+,(\w\w\w),.+?,.+?,(\w\w\w),.+?,.+?,(\d+),(\d+),(\d+),(\d+),(\d+)""".r
	  var flights : Array[Flight] = null;
	  Source.fromFile("flights/xaa").foreach { line => {
	      val newFlight = Pattern(year, dayOfMonth, flightDate, airlineId, carrier,
	          flightNumber, origin, destination, departureTime, arrivalTime, elapsedTime,
	          airTime, distance)
	          
	      val flight =  Flight(year, dayOfMonth, flightDate, airlineId, carrier,
	          flightNumber, origin, destination, departureTime, arrivalTime, elapsedTime,
	          airTime, distance)
	      
	    }
	  }
	  flights
	}
}