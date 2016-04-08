package com.objectivity.thingspan.examples.flights.dataload

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite

import com.objectivity.thingspan.examples.flights.model.AppConfig

class ResequenceTests extends FunSuite{
  
  val sc = new SparkContext(new SparkConf().setAppName("FlightResequence").setMaster("local[*]"))
  val sqlContext = new SQLContext(sc)

  
  test("Airport load JSON") {
 	  var airportsDF = sqlContext.read.json(AppConfig.DataDirectory +"/airports/json")
  }
  
  test("Airline load JSON") {
 	  var airlinesDF = sqlContext.read.json(AppConfig.DataDirectory +"/airlines/json")
  }
  
  test("Route load JSON") {
 	  var routesDF = sqlContext.read.json(AppConfig.DataDirectory +"/routes/json")
  }
  
  test("Flight load JSON") {
 	  var flightsDF = sqlContext.read.json(AppConfig.DataDirectory +"/flights/json")
  }
}