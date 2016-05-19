package com.objectivity.thingspan.examples.flights.model

import scala.beans.BeanProperty

import com.objy.data.Class
import com.objy.data.ClassBuilder
import com.objy.data.DataSpecification
import com.objy.data.Encoding
import com.objy.data.Instance
import com.objy.data.LogicalType
import com.objy.data.Storage
import com.objy.data.dataSpecificationBuilder.IntegerSpecificationBuilder
import com.objy.data.dataSpecificationBuilder.ListSpecificationBuilder
import com.objy.data.dataSpecificationBuilder.ReferenceSpecificationBuilder
import com.objy.data.schemaProvider.SchemaProvider
import com.objy.db.Transaction
import com.objy.db.TransactionMode

trait FlightsEdge
trait FlightsVertex

/*
 * Verticies
 */
case class Airline (

		@BeanProperty var airlineId: Int,    // Unique OpenFlights identifier for this airline.
		@BeanProperty var name: String,      // Name of the airline.
		@BeanProperty var alias: String,     // Alias of the airline. For example, All Nippon Airways is commonly known as "ANA".
		@BeanProperty var IATA: String,      // 2-letter IATA code, if available.
		@BeanProperty var ICAO: String,      // 3-letter ICAO code, if available.
		@BeanProperty var callsign: String,  // Airline callsign.
		@BeanProperty var country: String,   // Country or territory where airline is incorporated.
		@BeanProperty var active: String    // "Y" if the airline is or has until recently been operational, "N" if it is defunct. 
) extends FlightsVertex 

object Airline {
	val AIRLINE_CLASS_NAME = "com.objectivity.thingspan.examples.flights.model.Airline"

	def airlineFromCSV(source: String): Airline = {

			val p = source.split(",").map(_.trim)
			
			Airline(p(0).toInt,  
							Tools.trimQuotes(p(1)),       
							Tools.trimQuotes(p(2)),       
							Tools.trimQuotes(p(3)),       
							Tools.trimQuotes(p(4)),       
							Tools.trimQuotes(p(6)),       
							Tools.trimQuotes(p(6)),       
							Tools.trimQuotes(p(7))       
							)
	}
	
	def toPersistant(airline: Airline): Instance = {
	     var airlineInstance = Instance.createPersistent(Tools.fetchAirlineClass());
       airlineInstance.getAttributeValue("airlineId").set(airline.airlineId);
       airlineInstance.getAttributeValue("name").set(airline.name);
       airlineInstance.getAttributeValue("alias").set(airline.alias);
       airlineInstance.getAttributeValue("IATA").set(airline.IATA);
       airlineInstance.getAttributeValue("ICAO").set(airline.ICAO);
       airlineInstance.getAttributeValue("country").set(airline.country);
       airlineInstance.getAttributeValue("active").set(airline.active);
       airlineInstance
	}
}



case class Airport (
    

		@BeanProperty var airportId: Int,	// Unique identifier for this airport.
		@BeanProperty var name: String,			// Name of airport. May or may not contain the City name.
		@BeanProperty var city: String,			// Main city served by airport. May be spelled differently from Name.
		@BeanProperty var country: String,		// Country or territory where airport is located.
		@BeanProperty var IATA: String,			// 3-lettercode. Blank if not assigned.
		@BeanProperty var ICAO: String,			// 4-letter ICAO code. Blank if not assigned.
		@BeanProperty var latitude: Double,	// Decimal degrees, usually to six significant digits. Negative is South, positive is North.
		@BeanProperty var longitude: Double,	// Decimal degrees, usually to six significant digits. Negative is West, positive is East.
		@BeanProperty var altitude: Int,      // In feet.
		@BeanProperty var timezone: Double,	// Hours offset from UTC. Fractional hours are expressed as decimals, eg. India is 5.5. 
		@BeanProperty var DST: String,        // DST	Daylight savings time. One of E (Europe), A (US/Canada), S (South America), O (Australia), Z (New Zealand), N (None) or U (Unknown).
		@BeanProperty var tz: String               //Timezone in "tz" (Olson) format, eg. "America/Los_Angeles".
		) extends FlightsVertex

object Airport {
 	val AIRPORT_CLASS_NAME = "com.objectivity.thingspan.examples.flights.Airport"
 
  def enptyAirport(): Airport = {
					Airport(0,  
							null,       
							null,       
							null,       
							null,       
							null,       
							0.0,       
							0.0,       
							0,
							0.0,       
							null,
							null
							)
  }
  
 		def toPersistant(airport: Airport): Instance = {
	     var airportInstance = Instance.createPersistent(Tools.fetchAirportClass());
       airportInstance.getAttributeValue("airportId").set(airport.airportId);
       airportInstance.getAttributeValue("name").set(airport.name);
       airportInstance.getAttributeValue("city").set(airport.city);
       airportInstance.getAttributeValue("country").set(airport.country);
       airportInstance.getAttributeValue("IATA").set(airport.IATA);
       airportInstance.getAttributeValue("ICAO").set(airport.ICAO);
       airportInstance.getAttributeValue("latitude").set(airport.latitude);
       airportInstance.getAttributeValue("longitude").set(airport.longitude);
       airportInstance.getAttributeValue("altitude").set(airport.altitude);
       airportInstance.getAttributeValue("timezone").set(airport.timezone);
       airportInstance.getAttributeValue("DST").set(airport.DST);
       airportInstance.getAttributeValue("tz").set(airport.tz);
       airportInstance
	}

 	
	def airportFromCSV(source: String): Airport = {

			val p = source.split(",").map(_.trim)
					Airport(p(0).toInt,  
							Tools.trimQuotes(p(1)),       
							Tools.trimQuotes(p(2)),       
							Tools.trimQuotes(p(3)),       
							Tools.trimQuotes(p(4)),       
							Tools.trimQuotes(p(5)),       
							p(6).toDouble,       
							p(7).toDouble,
							p(8).toInt,
							p(9).toDouble,
							Tools.trimQuotes(p(10)),
							Tools.trimQuotes(p(11))
							)
	}
}

case class Route (

		@BeanProperty var airline: String,             // 2-letter (IATA) or 3-letter (ICAO) code of the airline.
		@BeanProperty var airlineId: Int,              //	Unique OpenFlights identifier for airline (see Airline).
		@BeanProperty var sourceAirport: String,       //	3-letter (IATA) or 4-letter (ICAO) code of the source airport.
		@BeanProperty var sourceAirportId: Int,        //	Unique OpenFlights identifier for source airport (see Airport)
		@BeanProperty var destinationAirport: String,  //	3-letter (IATA) or 4-letter (ICAO) code of the destination airport.
		@BeanProperty var destinationAirportId: Int,   // Unique OpenFlights identifier for destination airport (see Airport)
		@BeanProperty var codeshare: String,           //	"Y" if this flight is a codeshare (that is, not operated by Airline, but another carrier), empty otherwise.
		@BeanProperty var stops: Int,                  //	Number of stops on this flight ("0" for direct)
		@BeanProperty var equipment: String            //	3-letter codes for plane type(s) generally used on this flight, separated by spaces
) extends FlightsVertex
    
object Route {
	val ROUTE_CLASS_NAME = "com.objectivity.thingspan.examples.flights.model.Route"
  def toPersistant(route: Route): Instance = {
	     var routeInstance = Instance.createPersistent(Tools.fetchRouteClass());
	     routeInstance.getAttributeValue("airline").set(route.airline);
	     routeInstance.getAttributeValue("airlineId").set(route.airlineId);
	     routeInstance.getAttributeValue("sourceAirport").set(route.sourceAirport);
	     routeInstance.getAttributeValue("sourceAirportId").set(route.sourceAirportId);
	     routeInstance.getAttributeValue("destinationAirport").set(route.destinationAirport);
	     routeInstance.getAttributeValue("destinationAirportId").set(route.destinationAirportId);
	     routeInstance.getAttributeValue("codeshare").set(route.codeshare);
	     routeInstance.getAttributeValue("stops").set(route.stops);
	     routeInstance.getAttributeValue("equipment").set(route.equipment);
	     routeInstance
	}
  def routeFromCSV(source: String): Route = {
			val p = source.split(",").map(_.trim)
			
			Route(p(0),  
			   if (p(1).equalsIgnoreCase("\\N")) -1 else p(1).toInt,
					p(2),
			    if (p(3).equalsIgnoreCase("\\N")) -1 else p(3).toInt,
					p(4),
			    if (p(5).equalsIgnoreCase("\\N")) -1 else p(5).toInt,
					p(6),
			    p(7).toInt,
					p(6)
					)
	}
}

/*
 * Edges
 */

case class Flight (
		@BeanProperty var year: Int,
		@BeanProperty var dayOfMonth: Int,
		@BeanProperty var flightDate: String,
		@BeanProperty var airlineId: Int,
		@BeanProperty var carrier: String,
		@BeanProperty var flightNumber: Int,
		@BeanProperty var origin: String,
		@BeanProperty var destination: String,
		@BeanProperty var departureTime: String,
		@BeanProperty var arrivalTime: String,
		@BeanProperty var elapsedTime: Int,
		@BeanProperty var airTime: Int,
		@BeanProperty var distance: Int
		) extends FlightsEdge

object Flight {
  
  val FLIGHT_CLASS_NAME = "com.objectivity.thingspan.examples.flights.Flight"

  def toPersistant(flight: Flight): Instance = {
    var flightInstance = Instance.createPersistent(Tools.fetchFlightClass());
    flightInstance.getAttributeValue("year").set(flight.year);
    flightInstance.getAttributeValue("dayOfMonth").set(flight.dayOfMonth);
    flightInstance.getAttributeValue("flightDate").set(flight.flightDate);
    flightInstance.getAttributeValue("airlineId").set(flight.airlineId);
    flightInstance.getAttributeValue("carrier").set(flight.carrier);
    flightInstance.getAttributeValue("flightNumber").set(flight.flightNumber);
    flightInstance.getAttributeValue("origin").set(flight.origin);
    flightInstance.getAttributeValue("destination").set(flight.destination);
    flightInstance.getAttributeValue("departureTime").set(flight.departureTime);
    flightInstance.getAttributeValue("arrivalTime").set(flight.arrivalTime);
    flightInstance.getAttributeValue("elapsedTime").set(flight.elapsedTime);
    flightInstance.getAttributeValue("airTime").set(flight.airTime);
    flightInstance.getAttributeValue("distance").set(flight.distance);
    flightInstance
  }
  
	def flightFromCSV(source: String): Flight = {
			val Pattern = """^\d+,(\d+),(\d+),(\d\d\d\d/\d\d/\d\d),(\d+),(\w\w),(\d+),\d+,(\w\w\w),.+?,.+?,(\w\w\w),.+?,.+?,(\d+),(\d+),(\d+),(\d+),(\d+)""".r

					val  Pattern(year, dayOfMonth, flightDate, airlineId, carrier,
							flightNumber, origin, destination, departureTime, arrivalTime, elapsedTime,
							airTime, distance) = source

							Flight(year.toInt, dayOfMonth.toInt, flightDate, airlineId.toInt, carrier,
									flightNumber.toInt, origin, destination, padTime(departureTime), padTime(arrivalTime), timeStringToMinutes(elapsedTime),
									timeStringToMinutes(airTime), distance.toInt)
	}

	def padTime(time:String) : String = {
			if (time.length == 4)
				return time.substring(0,2) + ":" + time.substring(2) + ":00"
						else 
							return "0" + time.substring(0,1) + ":" + time.substring(1) + ":00"
	}

	def timeStringToMinutes(timeString: String) : Int = {
			var minutes = 0;
			if (timeString.length == 4){
				minutes = timeString.substring(0, 2).toInt * 60 + timeString.substring(2).toInt
			} else if (timeString.length == 3){
				minutes = timeString.substring(0, 1).toInt * 60 + timeString.substring(1).toInt
			} else {
				minutes = timeString.toInt
			}
			minutes
	}

	def formatDate(date:String) : String = {
			date.substring(0, 4) + "/" + date.substring(4, 6) + "/" + date.substring(6)
	}
}


/*
 * Tools
 */
object AppConfig { 
	var Boot = "data/flights.boot"
	var TestData = false
  var DataDirectory = "data"
  var port = 7777
  var host = "localhost"
  var time = 30
  var SparkMaster = "local[*]"
}


object Tools {
  
  def ltrimQuotes(s: String) = s.replaceAll("^\\\"", "")
  def rtrimQuotes(s: String) = s.replaceAll("\"$", "")
  def trimQuotes(s: String) = rtrimQuotes(ltrimQuotes(s))
  
  /**
   * register the persistent classes to create the Schema
   */
  def registerClasses() {
    
    
    println("Register Classes");

			val floatSpec = new com.objy.data.dataSpecificationBuilder.RealSpecificationBuilder(Storage.Real.B64)
                  .setEncoding(Encoding.Real.IEEE)
                  .build();
                  
			val provider = SchemaProvider.getDefaultPersistentProvider()
			var tx = new Transaction(TransactionMode.READ_UPDATE)
			try {
			  val flightClassBuilder = new ClassBuilder(
                Flight.FLIGHT_CLASS_NAME).setSuperclass("ooObj")
              .addAttribute(LogicalType.INTEGER, "year")
              .addAttribute(LogicalType.INTEGER, "dayOfMonth")
              .addAttribute(LogicalType.STRING, "flightDate")
              .addAttribute(LogicalType.INTEGER, "airlineId")
              .addAttribute(LogicalType.STRING, "carrier")
              .addAttribute(LogicalType.INTEGER, "flightNumber")
              .addAttribute(LogicalType.STRING, "origin")
              .addAttribute(LogicalType.STRING, "destination")
              .addAttribute(LogicalType.STRING, "departureTime")
              .addAttribute(LogicalType.STRING, "arrivalTime")
              .addAttribute(LogicalType.INTEGER, "elapsedTime")
              .addAttribute(LogicalType.INTEGER, "airTime")
              .addAttribute(LogicalType.INTEGER, "distance")
		    
        addToOne(flightClassBuilder, "destinationAirport", Airport.AIRPORT_CLASS_NAME, null);
        addToOne(flightClassBuilder, "originAirport", Airport.AIRPORT_CLASS_NAME, null);
        addToOne(flightClassBuilder, "airline", Airline.AIRLINE_CLASS_NAME, null);
        addToOne(flightClassBuilder, "route", Route.ROUTE_CLASS_NAME, null);
       
   	    println("\tCreated Flight schema");

   	    val airportClassBuilder = new ClassBuilder(
                 Airport.AIRPORT_CLASS_NAME).setSuperclass("ooObj")
              .addAttribute(LogicalType.INTEGER, "airportId")
              .addAttribute(LogicalType.STRING, "name")
              .addAttribute(LogicalType.STRING, "city")
              .addAttribute(LogicalType.STRING, "country")
              .addAttribute(LogicalType.STRING, "IATA")
              .addAttribute(LogicalType.STRING, "ICAO")
              .addAttribute("latitude", floatSpec)
   	          .addAttribute("longitude", floatSpec)
              .addAttribute(LogicalType.INTEGER, "altitude")
              .addAttribute("timezone", floatSpec)
              .addAttribute(LogicalType.STRING, "DST")
              .addAttribute(LogicalType.STRING, "tz")
              .addAttribute(LogicalType.INTEGER, "distance")
		    
        addToMany(airportClassBuilder, "inboundFlights", Flight.FLIGHT_CLASS_NAME, null);
        addToMany(airportClassBuilder, "outboundFlights", Flight.FLIGHT_CLASS_NAME, null);
   	    println("\tCreated Airport schema");

      val airlineClassBuilder = new ClassBuilder(
                   Airline.AIRLINE_CLASS_NAME).setSuperclass("ooObj")
          		.addAttribute(LogicalType.INTEGER, "airlineId")
          		.addAttribute(LogicalType.STRING, "name:")
          		.addAttribute(LogicalType.STRING, "alias")
          		.addAttribute(LogicalType.STRING, "IATA")
          		.addAttribute(LogicalType.STRING, "ICAO")
          		.addAttribute(LogicalType.STRING, "callsign")
          		.addAttribute(LogicalType.STRING, "country")
          		.addAttribute(LogicalType.STRING, "active")
          		
        addToMany(airlineClassBuilder, "flights", Flight.FLIGHT_CLASS_NAME, null);
      println("\tCreated Airline schema")
          		
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
        addToMany(routeClassBuilder, "flights", Flight.FLIGHT_CLASS_NAME, null);
      println("\tCreated Route schema")
      
      val flightClass = flightClassBuilder.build();
       println(flightClass.getName)
      val airportClass = airportClassBuilder.build();
       println(airportClass.getName)
      val airlineClass = airlineClassBuilder.build();
       println(airlineClass.getName)
      val routeClass = routeClassBuilder.build();
       println(routeClass.getName)
             
      provider.represent(flightClass)
      provider.represent(airportClass);
      provider.represent(airlineClass);
      provider.represent(routeClass);

      tx.commit()
      
      
		} catch {
  			case  e: Exception => {
  			  tx.abort()
    			e.printStackTrace();
    		}
    } finally {
  			  tx.close()
    }
  }
  
  def fetchAirlineClass():Class = {
     com.objy.data.Class.lookupClass(Airline.AIRLINE_CLASS_NAME)
  }
  
  def fetchAirportClass():Class = {
     com.objy.data.Class.lookupClass(Airport.AIRPORT_CLASS_NAME)
  }
  
  def fetchFlightClass():Class = {
     com.objy.data.Class.lookupClass(Flight.FLIGHT_CLASS_NAME)
  }
  
  def fetchRouteClass():Class = {
     com.objy.data.Class.lookupClass(Route.ROUTE_CLASS_NAME)
  }
  
  	/**
	 * Adds a one to many relationship on the specified class builder.
	 * @param builder The class to have the one to many relationship
	 * @param attrName The name of the to attribute
	 * @param otherClassName The class to be associated with the relationship (toMany)
	 * @param inverseName The name of the attribute the associated class will have to reference its parent.
	 */
	private def addToMany(builder: ClassBuilder, attrName:String, otherClassName:String, inverseName: String) {

		val intSpec = new IntegerSpecificationBuilder(Storage.Integer.B64)
	            .setEncoding(Encoding.Integer.UNSIGNED)
	            .build()
			   
		val refSpecBuilder = new ReferenceSpecificationBuilder()
				.setReferencedClass(otherClassName)
				.setIdentifierSpecification(intSpec)
		
		if(inverseName != null)
			refSpecBuilder.setInverseAttribute(inverseName)

		val dataSpec = new ListSpecificationBuilder()
				.setStorage(Storage.List.VARIABLE)
				.setElementSpecification(refSpecBuilder.build())
				.build()
		builder.addAttribute(attrName, dataSpec)
	}

	/**
	 * Adds a to one relationship on the specified class builder
	 * @param builder The class Builder to apply the relationship
	 * @param attrName The attribute name of the relationship on the specified class builder
	 * @param otherClassName The class to associate the relationship with
	 * @param inverseName The name of the inverse attribute
	 */
	private def addToOne(builder: ClassBuilder, attrName:String, otherClassName:String, inverseName: String) {

		val intSpec = new IntegerSpecificationBuilder(Storage.Integer.B64)
	            .setEncoding(Encoding.Integer.UNSIGNED)
	            .build()
		   
		val refSpecBuilder = new ReferenceSpecificationBuilder()
				.setReferencedClass(otherClassName)
				.setIdentifierSpecification(intSpec)
		
		if(inverseName != null)
			refSpecBuilder.setInverseAttribute(inverseName)

		val dataSpec = refSpecBuilder.build()
		builder.addAttribute(attrName, dataSpec)
	}

}