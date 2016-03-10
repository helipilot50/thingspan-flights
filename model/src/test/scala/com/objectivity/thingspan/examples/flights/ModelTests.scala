package com.objectivity.thingspan.examples.flights
import org.scalatest.FunSuite
import scala.io.Source

class ModelTests extends FunSuite {
  AppConfig.DataDirectory = "../data"

  test("Airport load") {
 	  Source.fromFile(AppConfig.DataDirectory +"/airports/airports.dat").getLines.foreach { line => {
//	    println(line)
	      val airport = Airport.airportFromString(line)
	    //println(airport)
				
	    }
 	  }
  }
  test("Airline load") {
 	  Source.fromFile(AppConfig.DataDirectory +"/airlines/airlines.dat").getLines.foreach { line => {
//	    println(line)
	      val airline = Airline.airlineFromString(line)
	    //println(airline)
				
	    }
 	  }
  }
  test("Route load") {
 	  Source.fromFile(AppConfig.DataDirectory +"/routes/routes.dat").getLines.foreach { line => {
//	    println(line)
	      val route = Route.routeFromString(line)
	    //println(route)
				
	    }
 	  }
  }
  test("Flights load") {
 	  Source.fromFile(AppConfig.DataDirectory +"/flights/xbr").getLines.foreach { line => {
//	    println(line)
	      val flight = Flight.flightFromString(line)
	    //println(flight)
				
	    }
 	  }
  }
}