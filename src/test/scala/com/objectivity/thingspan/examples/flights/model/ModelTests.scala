package com.objectivity.thingspan.examples.flights

import scala.io.Source
import org.scalatest.FlatSpec
import com.objectivity.thingspan.examples.flights.model.Airport
import com.objectivity.thingspan.examples.flights.model.AppConfig
import com.objectivity.thingspan.examples.flights.model.Airline
import com.objectivity.thingspan.examples.flights.model.Flight
import com.objectivity.thingspan.examples.flights.model.Route
import com.objectivity.thingspan.examples.flights.model.Tools

class ModelTests extends FlatSpec {
  

  "The Data" should "load Airport from CSV files" in {
 	  Source.fromFile("data/airports/csv/airports.csv").getLines.foreach { line => {
//	    println(line)
	      val airport = Airport.airportFromCSV(line)
	    //println(airport)
				
	    }
 	  }
  }
  it should "load Airline from CSV" in {
 	  Source.fromFile("data/airlines/csv/airlines.csv").getLines.foreach { line => {
//	    println(line)
	      val airline = Airline.airlineFromCSV(line)
	    //println(airline)
				
	    }
 	  }
  }
  it should "load Route from CSV" in {
 	  Source.fromFile("data/routes/csv/routes.csv").getLines.foreach { line => {
//	    println(line)
	      val route = Route.routeFromCSV(line)
	    //println(route)
				
	    }
 	  }
  }
  it should "Flights load CSV" in {
 	  Source.fromFile("data/flights/csv/xbr").getLines.foreach { line => {
//	    println(line)
	      val flight = Flight.flightFromCSV(line)
	    //println(flight)
				
	    }
 	  }
  }
  
  "The registration" should "create class definitions" in {
    Tools.registerClasses()
  }
}