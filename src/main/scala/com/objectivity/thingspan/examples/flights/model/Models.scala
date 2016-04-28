package com.objectivity.thingspan.examples.flights.model

import scala.beans.BeanProperty

import com.objy.db.app.OneToOne
import com.objy.db.app.ToOneRelationship
import com.objy.db.app.ooObj
import com.objy.db.app.Relationship
import com.objy.db.app.ToManyRelationship
import com.objy.db.app.OneToMany
import com.objy.db.DatabaseNotFoundException
import com.objy.db.ObjyRuntimeException
import com.objy.db.DatabaseClosedException
import com.objy.db.app.oo
import com.objy.db.app.Connection
import com.objy.db.DatabaseOpenException

trait FlightsEdge extends ooObj
trait FlightsVertex extends ooObj

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
		@BeanProperty var tz: String,               //Timezone in "tz" (Olson) format, eg. "America/Los_Angeles".
		@BeanProperty var inboundFlights: ToManyRelationship = null,
		@BeanProperty var outboundFlights: ToManyRelationship = null
		) extends FlightsVertex

object Airport {
  
  def inboundFlights_Relationship(): OneToMany = {
      val inbound = new OneToMany("inboundFlights",  // Relationship field
                  "com.objectivity.thingspan.examples.flights.model.Flight", // Destination class
                  "to",                      // Inverse relationship                    
                  Relationship.COPY_MOVE,    // Copying behavior
                  Relationship.VERSION_MOVE, // Versioning behavior
                  false,                     // Delete propagation
                  false,                     // Lock propagation
                  Relationship.INLINE_LONG)  // Storage mode
    inbound
  }

  def outboundFlights_Relationship(): OneToMany = {
      val outbound = new OneToMany("outboundFlights",  // Relationship field
                  "com.objectivity.thingspan.examples.flights.model.Flight", // Destination class
                  "from",                      // Inverse relationship                    
                  Relationship.COPY_MOVE,    // Copying behavior
                  Relationship.VERSION_MOVE, // Versioning behavior
                  false,                     // Delete propagation
                  false,                     // Lock propagation
                  Relationship.INLINE_LONG)  // Storage mode
    outbound
  }

  
  
  
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
		@BeanProperty var distance: Int,
		@BeanProperty var from: ToOneRelationship = null,
		@BeanProperty var to: ToOneRelationship = null
		) extends FlightsEdge

object Flight {
  
  def flyTo_Relationship(): OneToOne = {
    val flyTo = new OneToOne("to",         // Relationship field
                "com.objectivity.thingspan.examples.flights.model.Airport", // Destination class
                "flight",                  // Inverse relationship                    
                Relationship.COPY_MOVE,    // Copying behavior
                Relationship.VERSION_MOVE, // Versioning behavior
                false,                     // Delete propagation
                false,                     // Lock propagation
                Relationship.INLINE_LONG)  // Storage mode
    flyTo
  }
  def flyFrom_Relationship(): OneToOne = {
    val flyFrom = new OneToOne("from",     // Relationship field
                "com.objectivity.thingspan.examples.flights.model.Airport", // Destination class
                "flight",                  // Inverse relationship                    
                Relationship.COPY_MOVE,    // Copying behavior
                Relationship.VERSION_MOVE, // Versioning behavior
                false,                     // Delete propagation
                false,                     // Lock propagation
                Relationship.INLINE_LONG)  // Storage mode
    flyFrom
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
   * register the persistent classes
   */
  def registerClasses() {
    
    println("Flights Register Classes");

			//	connect to federation
			try {
				println("\tOpen Connection()")
				var connection = Connection.open(AppConfig.Boot, oo.openReadWrite)
				
			  println("\tRegister Classes")
			  connection.useContextClassLoader(true)
		    connection.registerClass(classOf[Airport].getName)
        connection.registerClass(classOf[Flight].getName)
		    
			  println("\tClose Connection");
				connection.close();
			} catch {
			  case  dnfe: DatabaseNotFoundException => {
      				println("Federated database not found - use objy newfd to create federated database.")
      				println("Error: " + dnfe.getMessage())
      			}
			  
  			case doe: DatabaseOpenException => {
  				println("Error: " + doe.getMessage());
  			}
  
  			case e: DatabaseClosedException => {
  				e.printStackTrace();
  			}
  		
  			case  ore: ObjyRuntimeException => {
    			println("ObjyRuntimeException: " + ore.getMessage());
    			ore.printStackTrace();
    		}
     }
  }
}