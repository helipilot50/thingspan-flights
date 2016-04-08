package com.objectivity.thingspan.examples.flights
import org.scalatest.FunSuite
import scala.io.Source
import com.objectivity.thingspan.examples.flights.model.Flight
import com.objectivity.thingspan.examples.flights.model.AppConfig
import com.objectivity.thingspan.examples.flights.model.Route
import com.objectivity.thingspan.examples.flights.model.Airline
import com.objectivity.thingspan.examples.flights.model.Airport

class ModelTests extends FunSuite {
  AppConfig.DataDirectory = "data"

  test("Airport load CSV") {
 	  Source.fromFile(AppConfig.DataDirectory +"/airports/csv/airports.csv").getLines.foreach { line => {
//	    println(line)
	      val airport = Airport.airportFromCSV(line)
	    //println(airport)
				
	    }
 	  }
  }
  test("Airline load CSV") {
 	  Source.fromFile(AppConfig.DataDirectory +"/airlines/csv/airlines.csv").getLines.foreach { line => {
//	    println(line)
	      val airline = Airline.airlineFromCSV(line)
	    //println(airline)
				
	    }
 	  }
  }
  test("Route load CSV") {
 	  Source.fromFile(AppConfig.DataDirectory +"/routes/csv/routes.csv").getLines.foreach { line => {
//	    println(line)
	      val route = Route.routeFromCSV(line)
	    //println(route)
				
	    }
 	  }
  }
  test("Flights load CSV") {
 	  Source.fromFile(AppConfig.DataDirectory +"/flights/csv/xbr").getLines.foreach { line => {
//	    println(line)
	      val flight = Flight.flightFromCSV(line)
	    //println(flight)
				
	    }
 	  }
  }
}