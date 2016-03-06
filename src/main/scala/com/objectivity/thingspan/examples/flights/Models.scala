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
		@BeanProperty var elapsedTime: String,
		@BeanProperty var airTime: String,
		@BeanProperty var distance: Int
		)
		
object Constants { 
 val Boot = "/vagrant/data/flights.boot"
}
