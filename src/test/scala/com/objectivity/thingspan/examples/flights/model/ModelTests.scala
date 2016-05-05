package com.objectivity.thingspan.examples.flights

import scala.io.Source
import org.scalatest.FlatSpec
import com.objectivity.thingspan.examples.flights.model.Airport
import com.objectivity.thingspan.examples.flights.model.AppConfig
import com.objectivity.thingspan.examples.flights.model.Airline
import com.objectivity.thingspan.examples.flights.model.Flight
import com.objectivity.thingspan.examples.flights.model.Route
import com.objectivity.thingspan.examples.flights.model.Tools
import com.objy.db.Connection
import com.objy.db.TransactionScopeOption
import com.objy.db.TransactionMode
import com.objy.db.TransactionScope
import org.scalatest.Ignore
import com.objy.db.Transaction



class ModelTests extends FlatSpec {
  
		val connection = new Connection(AppConfig.Boot);


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
    com.objy.db.Objy.startup()
    val connection = new Connection(AppConfig.Boot)
    Tools.registerClasses()
    com.objy.db.Objy.shutdown()
  }
  
  it should "Verify class definition" in {
    com.objy.db.Objy.startup();
    val connection = new Connection(AppConfig.Boot)
      val tx = new Transaction(TransactionMode.READ_ONLY)
  
      assert(Tools.fetchFlightClass()!=null)
      assert(Tools.fetchAirportClass()!=null)
      assert(Tools.fetchAirlineClass()!=null)
      assert(Tools.fetchRouteClass()!=null)
      
      tx.commit()
      com.objy.db.Objy.shutdown();
  }

  
  
}