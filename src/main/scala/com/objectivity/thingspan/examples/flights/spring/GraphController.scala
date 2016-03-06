package com.objectivity.thingspan.examples.flights.spring

import org.springframework.ui.Model
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.ResponseBody
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import com.objectivity.thingspan.examples.flights.Constants
import com.objectivity.thingspan.examples.flights.Airline


@RestController
@RequestMapping(Array("/flights"))
class GraphController {

	@Autowired
	var sc : SparkContext = null
	var sql : SQLContext = null


	def sqlContext() : SQLContext = {
			if (sql == null)
				sql = new SQLContext(sc);
			sql
	}

	@RequestMapping(Array("/{date}"))
	@ResponseBody
	def list(@PathVariable("date") date : String) = {

//			val dateFormated = date.substring(0, 3) + "/" + date.substring(4, 5) + "/" + date.substring(6)
//
//			val flightsDF = sqlContext.read.
//  			format("com.objy.spark.sql").
//  			option("objy.bootFilePath", Constants.Boot).
//  			option("objy.dataClassName", "com.objectivity.thingspan.examples.flights.Flight").
//  			option("objy.addOidColumn", "flightOid").
//  			load 
//  			
//			flightsDF.registerTempTable("flightsTable")
//
//			val flightsQuery = """SELECT filghtDate,
//			| airlineId,
//			| flightNumber,
//			| origin,
//			| destination
//			| FROM flightsTable
//			| WHERE flightDate = """ + dateFormated
//
//			val flightForDayDF = sqlContext.sql(flightsQuery)
//			val flightsJson = flightForDayDF.toJSON
//			println(flightsJson)
//			flightsJson
	  TestData.someFlights()
	}
	

}