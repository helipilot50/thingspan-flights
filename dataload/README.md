# US Domestic Flights

## Install Thingspan jar
Install the thingspan jar to a local repo - only once
	
	mvn deploy:deploy-file -Durl=file://repo -Dfile=lib/spark-rdd-1.0.0-all.jar -DgroupId=com.objectivity -DartifactId=thingspan-spark-rdd -Dpackaging=jar -Dversion=1.0.0

## Loading Reference Data
Reference data are Airports, Airlines and Routes. This data only needs to be loaded once.

Use the following command to load reference data:

```bash
	spark-submit \
  	--master local[*] \
  	--class com.objectivity.thingspan.examples.flights.ReferenceData \
	--total-executor-cores 10 \
	--executor-memory 10g \
	thingspan-flights-dataload-1.0.0.jar -d data -b flights.boot
```
## Loading Flights
Flights are events that connect Airlines and Airports. In a graph, Airports and Airlines are vertices and Flights are edges.

Flights are added using the following command:
```bash
	spark-submit \
  	--master Local[1] \
  	--class com.objectivity.thingspan.examples.flights.FlightsLoader \
	--total-executor-cores 1 \
	--executor-memory 10g \
	thingspan-flights-dataload-1.0.0.jar -d data -b flights.boot
```


## Loading Flights with streaming

Start the streaming loader using the following command:
```bash
	nc -lk <port>&

	spark-submit \
  	--master Local[1] \
  	--class com.objectivity.thingspan.examples.flights.FlightsStream \
	--total-executor-cores 1 \
	--executor-memory 10g \
	thingspan-flights-dataload-1.0.0.jar -h <host> -p <port> -t 30 -b flights.boot
```
Run the nc command using:
```bash
	
	cd <data direcrtory/flights>
	cat * | nc <host> <port>
```
