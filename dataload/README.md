# US Domestic Flights


## Loading Data
```
	spark-shell --driver-memory 10G --jars ./thingspan-flights-1.0.0.jar,./spark-rdd-1.0.0-all.jar --class com.objectivity.thingspan.examples.flights.FlightsLoader 
```

## Creating Relationships
```
	spark-shell --driver-memory 10G --jars ./thingspan-flights-1.0.0.jar,./spark-rdd-1.0.0-all.jar --class com.objectivity.thingspan.examples.flights.CreateRelationships 
```
