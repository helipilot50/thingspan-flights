package com.objectivity.thingspan.examples.flights.query

import com.objectivity.thingspan.examples.flights.model.Flight
import com.objectivity.thingspan.examples.flights.model.Airport
import com.objectivity.thingspan.examples.flights.model.AppConfig
import com.objy.data.Attribute
import com.objy.data.Instance
import com.objy.data.Variable
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
import com.objy.db.Connection
import org.apache.spark.SparkConf


object FlightService {

  var connection: Connection = null;
  
	val FLIGHT_CLASS_NAME = "com.objectivity.thingspan.examples.flights.Flight"
	val AIRPORT_CLASS_NAME = "com.objectivity.thingspan.examples.flights.Airport"
	val AIRLINE_CLASS_NAME = "com.objectivity.thingspan.examples.flights.model.Airline"
	val ROUTE_CLASS_NAME = "com.objectivity.thingspan.examples.flights.model.Route"


  def listFlightsFrom(sc: SparkContext, from:String, lowDateTime : String, highDateTime : String, degree:Int): (RDD[Flight], RDD[(String, (Int, Airport))]) = {
    println(s"... listFlightsFrom($sc, $from, $lowDateTime, $highDateTime, $degree)")	
    if (connection == null){
      connection = new Connection(AppConfig.Boot)
    }

    var flightsRDD = sc.emptyRDD[Flight]
    var airportsRDD = sc.emptyRDD[(String, (Int, Airport))]
    
    val tx = new TransactionScope(TransactionMode.READ_ONLY, 
		    "spark_read", 
		    TransactionScopeOption.REQUIRED)
    println(s"... new Tx Scope $tx")	
		try {
    
      val  airportClass = com.objy.data.Class.lookupClass(AIRPORT_CLASS_NAME)
      val  flightClass = com.objy.data.Class.lookupClass(FLIGHT_CLASS_NAME)
  	  val  airlineClass = com.objy.data.Class.lookupClass(AIRLINE_CLASS_NAME)
  	  val  routeClass = com.objy.data.Class.lookupClass(ROUTE_CLASS_NAME)
  	  val  ooObjClass = com.objy.data.Class.lookupClass("ooObj")
  
      val destination = flightClass.lookupAttribute("destination");
    
		  
			val flightsFrom = airportClass.lookupAttribute("m_Transactions")

			// build an expression tree
			var opExp = new OperatorExpressionBuilder("TrailsFrom")
			// fromObjects
			.addOperator(new OperatorExpressionBuilder("FROM")
					.addLiteral(new Variable(airportClass))
					.addOperator(new OperatorExpressionBuilder("==")
							.addObjectValue("origin")
							.addVariable("from")
							)
					)
			// Path operand
			.addOperator(new OperatorExpressionBuilder("FollowedBy")
					// QualifyStep from Basket to Transaction
					.addOperator(qualifyStep(0,				// max skip
							flightClass,					// from object class
							destination,					// step attribute
							airportClass))				// to class
					.addOperator(new OperatorExpressionBuilder("Repeat")
							// min
							.addLiteral(new Variable(1))
							// max
							.addLiteral(new Variable(degree))
							)// end Repeat

					).build()
					
			var exprTreeBuilder = new ExpressionTreeBuilder(opExp)
			var exprTree = exprTreeBuilder.build(LanguageRegistry.lookupLanguage("DO"))
			println(s"... Expression tree: $exprTree")	


			// set variable values in the expression tree
			exprTree.setVariableValue("from", new Variable(from));
			
			//Create a statement
			var statement = new Statement(exprTree)
			
			//Execute statement
			val results = statement.execute()
			
			val pathItr = results.sequenceValue().iterator()
			
			while(pathItr.hasNext()){
			  println(pathItr.next())
			}
			
			tx.complete();
			
		} catch {
		case ex: Exception => {
  			println("Exception Thrown - " + ex.getMessage)
  			println(ex.printStackTrace())
  		}
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
}