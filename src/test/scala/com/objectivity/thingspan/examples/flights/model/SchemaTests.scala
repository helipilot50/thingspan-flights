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
import com.objy.data.ClassBuilder
import com.objy.data.schemaProvider.SchemaProvider
import com.objy.data.LogicalType
import org.scalatest.Matchers
import com.objy.db.Transaction

class SchemaTests extends FlatSpec with Matchers{
  
  "The Schema test" should "create class definitions" in {
    com.objy.db.Objy.startup();
    val connection = new Connection("data/flights.boot")
    connection should not be null
    
    val provider = SchemaProvider.getDefaultPersistentProvider()
    provider should not be null
    
//    val tx = new TransactionScope(TransactionMode.READ_UPDATE, 
//        "schema test", 
//        TransactionScopeOption.REQUIRED)
    var trx = new Transaction(TransactionMode.READ_UPDATE) 
    trx should not be null
    
    val routeClassBuilder = new ClassBuilder(
                   Route.ROUTE_CLASS_NAME).setSuperclass("ooObj")
              .addAttribute(LogicalType.STRING, "airline")
              .addAttribute(LogicalType.INTEGER, "airlineId")
              .addAttribute(LogicalType.STRING, "sourceAirport")
              .addAttribute(LogicalType.INTEGER, "sourceAirportId")
              .addAttribute(LogicalType.STRING, "destinationAirport")
              .addAttribute(LogicalType.INTEGER, "destinationAirportId")
              .addAttribute(LogicalType.STRING, "codeshare")
              .addAttribute(LogicalType.INTEGER, "stops")
              .addAttribute(LogicalType.STRING, "equipment")
    val routeClass = routeClassBuilder.build()
    provider.represent(routeClass)
      
    trx.commit()
    
    trx = new Transaction(TransactionMode.READ_ONLY)
    val rou = com.objy.data.Class.lookupClass(routeClass.getName) 
    rou should not be null
    trx.commit()
    com.objy.db.Objy.shutdown();
  
  }
  
  
}