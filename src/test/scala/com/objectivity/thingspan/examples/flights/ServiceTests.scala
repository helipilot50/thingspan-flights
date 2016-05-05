package com.objectivity.thingspan.examples.flights

import org.scalatest.FlatSpec
import com.objectivity.thingspan.examples.flights.query.FlightService
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.Ignore


class ServiceTests extends FlatSpec {
    "The Flignts service" should "find an Airport from IATA id and all the flights" in {
      var conf = new SparkConf().setMaster("local[*]")
					conf.setAppName("FlightService test")
					conf.set("spark.serializer.extraDebugInfo", "false")
					val sc = new SparkContext(conf)
					

      val result = FlightService.listFlightsFrom(sc, "LAX", "201201230000", "201201230900", 1)
    }
}