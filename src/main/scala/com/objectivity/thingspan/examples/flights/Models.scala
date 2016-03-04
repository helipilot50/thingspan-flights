package com.objectivity.thingspan.examples.flights

case class Airport(
		id: String,
		city: String,
		state: String
		)

case class Airline(
		id: Int,
		name: String 
		)

case class Flight (
		year: Long,
		dayOfMonth: Int,
		filghtDate: String,
		airlineId: Int,
		flightNumber: Int,
		origin: String,
		destination: String,
		departureTime: String,
		arrivalTime: String,
		elapsedTime: String,
		airTime: String,
		distance: Int
		)
		
object Constants { 
 val Boot = "/vagrant/data/flights.boot"
}
