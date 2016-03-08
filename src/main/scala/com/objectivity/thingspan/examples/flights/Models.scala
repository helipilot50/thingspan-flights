package com.objectivity.thingspan.examples.flights

import scala.beans.BeanProperty

case class Airport(
		@BeanProperty var id: String,
		@BeanProperty var city: String,
		@BeanProperty var state: String
		)

case class Airline(
		@BeanProperty var id: Int,
		@BeanProperty var name: String 
		)

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
		)

object Constants { 
	val Boot = "/vagrant/data/flights.boot"
}

object Flight {
	def flightFromString(source: String) : Flight = {
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