package com.objectivity.thingspan.examples.flights.query

import com.objectivity.thingspan.examples.flights.model.Flight
import com.objectivity.thingspan.examples.flights.model.Airline
import com.objectivity.thingspan.examples.flights.model.Airport
import com.objectivity.thingspan.examples.flights.model.Route
import com.objectivity.thingspan.examples.flights.model.AppConfig
import com.objy.data.Attribute
import com.objy.data.Instance
import com.objy.data.List
import com.objy.data.Reference
import com.objy.data.Sequence
import com.objy.data.Variable
import com.objy.db.Connection
import com.objy.db.TransactionMode
import com.objy.db.TransactionScope
import com.objy.db.TransactionScopeOption
import com.objy.expression.ExpressionTree
import com.objy.expression.ExpressionTreeBuilder
import com.objy.expression.OperatorExpression
import com.objy.expression.OperatorExpressionBuilder
import com.objy.expression.language.LanguageRegistry
import com.objy.statement.Statement
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import com.objectivity.thingspan.examples.flights.model.Tools
import com.objectivity.thingspan.examples.flights.model.Airline
import com.objy.db.Transaction
import com.objy.data.LogicalType


object FlightService {
  
  com.objy.db.Objy.startup();

  var connection: Connection = null;
  
  def listFlightsFrom(sc: SparkContext, from:String, lowDateTime : String, highDateTime : String, degree:Int): (RDD[Flight], RDD[(String, (Int, Airport))]) = {
    println(s"... listFlightsFrom($sc, $from, $lowDateTime, $highDateTime, $degree)")	
    if (connection == null){
      connection = new Connection(AppConfig.Boot)
    }

    var flightsRDD = sc.emptyRDD[Flight]
    var airportsRDD = sc.emptyRDD[(String, (Int, Airport))]
    
    val tx = new Transaction(TransactionMode.READ_ONLY)
    println(s"... new Tx $tx")	
		try {
    
      val  airportClass = Tools.fetchAirportClass()
      val  flightClass = Tools.fetchFlightClass()
      
//      val  airlineClass = com.objy.data.Class.lookupClass(Airline.AIRLINE_CLASS_NAME)
//  	  val  routeClass = com.objy.data.Class.lookupClass(Route.ROUTE_CLASS_NAME)
//      val  ooObjClass = com.objy.data.Class.lookupClass("ooObj")
  
      val destination = flightClass.lookupAttribute("destination");
    
			val flightsFrom = airportClass.lookupAttribute("inboundFlights")

			// build an expression tree
			var opExp = new OperatorExpressionBuilder("From")
			// fromObjects
					.addLiteral(new Variable(airportClass))
					.addOperator(new OperatorExpressionBuilder("==")
							.addObjectValue("IATA")
							.addLiteral(new Variable(from))
							)
				  .build()
					
			var exprTree = new ExpressionTreeBuilder(opExp)
			   .build(LanguageRegistry.lookupLanguage("DO"))
			//println(s"... Expression tree: $exprTree")	

			//Create a statement
			var statement = new Statement(exprTree)
			
			//Execute statement
			val results = statement.execute()
			
			val pathItr = results.sequenceValue().iterator()
			
			while(pathItr.hasNext()){
			  
			  val airportInstance = pathItr.next().instanceValue()
			  val airportIata = airportInstance.getAttributeValue("IATA").stringValue()
			  val airportName = airportInstance.getAttributeValue("name").stringValue()
			  val airportCity = airportInstance.getAttributeValue("city").stringValue()
			  println(s"... Result: $airportIata - $airportName in $airportCity")
			  val outboundFlights = airportInstance.getAttributeValue("outboundFlights")
			  
			   println(variableToString(outboundFlights))
			}
			
			tx.commit();
			
		} finally{
		  tx.close()
		}
    (flightsRDD, airportsRDD)
	}

	def qualifyStep(maxSkip: Long, fromClass: com.objy.data.Class, stepAttribute: Attribute, toClass: com.objy.data.Class ): OperatorExpressionBuilder = {
	  val  ooObjClass = com.objy.data.Class.lookupClass("ooObj")
			return new OperatorExpressionBuilder("QualifyStep")
					.addLiteral(new Variable(maxSkip))   		// max skip
					.addLiteral(new Variable(fromClass)) 		// from object class
					.addLiteral(new Variable(true))         	// from predicate
					.addLiteral(new Variable(stepAttribute))	// step attribute
					.addLiteral(new Variable(ooObjClass))   	// step data class
					.addLiteral(new Variable(true))         	// step data predicate
					.addLiteral(new Variable(toClass)) 			// to class
					.addLiteral(new Variable(true));			// to predicate
	}


	def toTime(lowDateTime: String, highDateTime: String): (String,String, String, String) = {
			val lowDate = Flight.formatDate(lowDateTime.substring(0,8))
					val lowTime = Flight.padTime(lowDateTime.substring(8))
					val highDate = Flight.formatDate(highDateTime.substring(0,8))
					val highTime = Flight.padTime(highDateTime.substring(8))
					(lowDate, lowTime, highDate, highTime)
	}
	
	def zuluTest() {
	  val conf = new SparkConf().setMaster(AppConfig.SparkMaster)
					conf.setAppName("Zulu Test")
					conf.set("spark.serializer.extraDebugInfo", "false")
					conf.setMaster(AppConfig.SparkMaster)
		val sc = new SparkContext(conf)
	  FlightService.listFlightsFrom(sc, "LAX", "201201010000", "201201012359", 1)
	}
	
	def variableToString(attributeValue: Variable): String = {
	  val varType = attributeValue.getSpecification().getLogicalType()
	  var asString: String = varType match {
      case LogicalType.INTEGER => java.lang.Long.toString(attributeValue.longValue())
      case LogicalType.REFERENCE => {
                val ref = attributeValue.referenceValue();
                if (!ref.isNull)
                  ref.getObjectId().toString()
                else 
                  null
            }
      case LogicalType.CHARACTER => Character.toString(attributeValue.charValue())
      case LogicalType.STRING => attributeValue.stringValue()
      case LogicalType.REAL => java.lang.Double.toString(attributeValue.doubleValue())
      case LogicalType.LIST => {
        val listVal = attributeValue.listValue()
        val size = listVal.size()
        "A list of size: " + size
        }
      case _ =>  null
    }
	  asString
  }

}