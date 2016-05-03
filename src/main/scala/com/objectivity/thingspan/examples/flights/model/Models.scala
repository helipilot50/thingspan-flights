package com.objectivity.thingspan.examples.flights.model

import scala.beans.BeanProperty
import com.objy.db.DatabaseNotFoundException
import com.objy.db.ObjyRuntimeException
import com.objy.db.DatabaseClosedException
import com.objy.db.DatabaseOpenException
import com.objy.data.schemaProvider.SchemaProvider
import com.objy.db.TransactionMode
import com.objy.db.TransactionScope
import com.objy.data.ClassBuilder
import com.objy.data.LogicalType
import com.objy.data.Storage
import com.objy.data.dataSpecificationBuilder.IntegerSpecificationBuilder
import com.objy.data.Encoding
import com.objy.data.dataSpecificationBuilder.ReferenceSpecificationBuilder
import com.objy.data.DataSpecification
import com.objy.data.dataSpecificationBuilder.ListSpecificationBuilder

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
   * register the persistent classes
   */
  def registerClasses() {
    
    println("Flights Register Classes");

			
			val provider = SchemaProvider.getDefaultPersistentProvider()
			val txScope = new TransactionScope(TransactionMode.READ_UPDATE)
			
			try {
			  println("\tRegister Classes")
			  val flightClassBuilder = new ClassBuilder(
                 "com.objectivity.thingspan.examples.flights.model.Flight")
              .setSuperclass("ooObj")
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
              .addAttribute(LogicalType.INTEGER, "arrivalTime")
              .addAttribute(LogicalType.INTEGER, "airTime")
              .addAttribute(LogicalType.INTEGER, "distance")
		    
        addToOne(flightClassBuilder, "to", "com.objectivity.thingspan.examples.flights.model.Airport", "inboundFlights");
        addToOne(flightClassBuilder, "from", "com.objectivity.thingspan.examples.flights.model.Airport", "outboundFlights");
        
   	    println("\tCreated Flight schema");

   	    val airportClassBuilder = new ClassBuilder(
                 "com.objectivity.thingspan.examples.flights.model.Airport")
              .setSuperclass("ooObj")
              .addAttribute(LogicalType.INTEGER, "airportId")
              .addAttribute(LogicalType.STRING, "name")
              .addAttribute(LogicalType.STRING, "city")
              .addAttribute(LogicalType.STRING, "country")
              .addAttribute(LogicalType.STRING, "IATA")
              .addAttribute(LogicalType.STRING, "ICAO")
              .addAttribute(LogicalType.REAL, "latitude")
              .addAttribute(LogicalType.REAL, "longitude")
              .addAttribute(LogicalType.INTEGER, "altitude")
              .addAttribute(LogicalType.REAL, "timezone")
              .addAttribute(LogicalType.STRING, "DST")
              .addAttribute(LogicalType.STRING, "tz")
              .addAttribute(LogicalType.INTEGER, "distance")
		    
        addToMany(airportClassBuilder, "inboundFlights", "com.objectivity.thingspan.examples.flights.model.Flight", "to");
        addToMany(airportClassBuilder, "outboundFlights", "com.objectivity.thingspan.examples.flights.model.Flight", "from");
   	    println("\tCreated Airport schema");

      val airlineClassBuilder = new ClassBuilder(
                   "com.objectivity.thingspan.examples.flights.model.Airline")
              .setSuperclass("ooObj")
          		.addAttribute(LogicalType.INTEGER, "airlineId")
          		.addAttribute(LogicalType.STRING, "name:")
          		.addAttribute(LogicalType.STRING, "alias")
          		.addAttribute(LogicalType.STRING, "IATA")
          		.addAttribute(LogicalType.STRING, "ICAO")
          		.addAttribute(LogicalType.STRING, "callsign")
          		.addAttribute(LogicalType.STRING, "country")
          		.addAttribute(LogicalType.STRING, "active")
      println("\tCreated Airline schema")
          		
      val routeClassBuilder = new ClassBuilder(
                   "com.objectivity.thingspan.examples.flights.model.Route")
              .setSuperclass("ooObj")
              .addAttribute(LogicalType.STRING, "airline")
              .addAttribute(LogicalType.INTEGER, "airlineId")
              .addAttribute(LogicalType.STRING, "sourceAirport")
              .addAttribute(LogicalType.INTEGER, "sourceAirportId")
              .addAttribute(LogicalType.STRING, "destinationAirport")
              .addAttribute(LogicalType.INTEGER, "destinationAirportId")
              .addAttribute(LogicalType.STRING, "codeshare")
              .addAttribute(LogicalType.INTEGER, "stops")
              .addAttribute(LogicalType.STRING, "equipment")
      println("\tCreated Route schema")
      
      val flightClass = flightClassBuilder.build();
      val airportClass = airportClassBuilder.build();
      val airlineClass = airlineClassBuilder.build();
      val routeClass = routeClassBuilder.build();
             
      provider.represent(flightClass);
      provider.represent(airportClass);
      provider.represent(airlineClass);
      provider.represent(routeClass);

       txScope.complete();
		} catch {
  			case  e: Exception => {
    			e.printStackTrace();
    		}
     } 
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