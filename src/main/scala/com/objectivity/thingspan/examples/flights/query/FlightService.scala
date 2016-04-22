package com.objectivity.thingspan.examples.flights.query

object FlightService {
  
  val FLIGHT_CLASS_NAME = "com.objectivity.thingspan.examples.flights.Flight"
  val AIRPORT_CLASS_NAME = "com.objectivity.thingspan.examples.flights.Airport"
  val AIRLINE_CLASS_NAME = "com.objectivity.thingspan.examples.flights.model.Airline"
  val ROUTE_CLASS_NAME = "com.objectivity.thingspan.examples.flights.model.Route"
  val airportClass = com.objy.data.Class.lookupClass(AIRPORT_CLASS_NAME)
	val flightClass = com.objy.data.Class.lookupClass(FLIGHT_CLASS_NAME)
	val airlineClass = com.objy.data.Class.lookupClass(AIRLINE_CLASS_NAME)
	val routeClass = com.objy.data.Class.lookupClass(ROUTE_CLASS_NAME)
	
  

  def flightsFrom()  {
    
    try (TransactionScope tx = new TransactionScope(TransactionMode.READ_ONLY, 
        "spark_read", TransactionScopeOption.REQUIRED)) {
			Attribute mTransactions = basketClass.lookupAttribute("m_Transactions");
			Attribute mEvents = transactionClass.lookupAttribute("m_Events");
			Attribute mChild = transactionClass.lookupAttribute("m_Children");


			OperatorExpression opExp = new OperatorExpressionBuilder("TrailsFrom")
			// fromObjects
			.addOperator(new OperatorExpressionBuilder("FROM")
					.addLiteral(new Variable(basketClass))
					.addOperator(new OperatorExpressionBuilder("==")
							.addObjectValue("m_Id")
							.addVariable("basketId")
					)
			)
			// Path operand
			.addOperator(new OperatorExpressionBuilder("FollowedBy")
					// QualifyStep from Basket to Transaction
					.addOperator(qualifyStep(0,				// max skip
							basketClass,					// from object class
							mTransactions,					// step attribute
							transactionClass))				// to class
					.addOperator(new OperatorExpressionBuilder("Repeat")
							// min
							.addLiteral(new Variable(1))
							// max
							.addLiteral(new Variable(15))
							.addOperator(new OperatorExpressionBuilder("OR")
									// QualifyStep from Transaction to TransactionEvent
								.addOperator(qualifyStep(0,							// max skip		
														 transactionClass,			// from object class
														 mEvents,					// step attribute
														 transactionEventClass))	// to class

								.addOperator(qualifyStep(0,						// max skip
														 transactionClass,		// from object class
														 mChild,				// step attribute
														 transactionClass)) 	// to class
								)// end OR
									)// end Repeat
						 	
			).build();

			ExpressionTreeBuilder exprTreeBuilder = new ExpressionTreeBuilder(opExp);
			ExpressionTree exprTree = exprTreeBuilder.build(LanguageRegistry.lookupLanguage("DO"));

			while (argItr.hasNext()) {
				String args = argItr.next();
				if (args != null) {
					queryInfoList.add(basketToService(args, exprTree));
				}
			}

			tx.complete();
			return queryInfoList;
    		}
  }
}