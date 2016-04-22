# ThingSpan Flights

This example is a demonstration of ThingSpan processing and visuializing domestic flight data.

## Install thingspan
Install ThingSpan by running the installer

	sudo ./installer-version-linux86_64.run

Start lock server

	objy StartLockServer

Change directory to your flights directory and create a database
 
	objy CreateFd -fdName flights -fdDirPath data


## Install Thingspan jar 
Install the thingspan jar to a local repo - only once
```bash
	mvn deploy:deploy-file -Durl=file://repo -Dfile=lib/spark-rdd-1.0.0-all.jar -DgroupId=com.objectivity -DartifactId=thingspan-spark-rdd -Dpackaging=jar -Dversion=1.0.0
```

## Build
The application is built using Maven

	mvn clean package

## Run
The build processes creates a single runnable mega JAR. The options to run this JAR are:

	usage: java -jar thingspan-flights-<version>.jar
	 -b,--boot <arg>     Boot file
	 -d,--data <arg>     Data directory
	 -i,--input          Load data
	 -m,--master <arg>   Spark Master default: local[*]
	 -r,--relation       Build relationships
	 -t,--test           Test data
	 -v,--view           View graph

### Loading Reference Data
Reference data are Airports, Airlines and Routes. This data only needs to be loaded once.

Use the following command to load reference data:

	java -cp target/thingspan-flights-1.0.0-SNAPSHOT.jar com.objectivity.thingspan.examples.flights.ThingSpanFlights -d data -b data/flights.boot -r

	

### Loading Flights
Flights are events that connect Airlines and Airports. In a graph, Airports and Airlines are vertices and Flights are edges.

Flights are added using the following command:

	java -cp target/thingspan-flights-1.0.0-SNAPSHOT.jar com.objectivity.thingspan.examples.flights.ThingSpanFlights -d data -b data/flights.boot -f

Relationships between flights and airports will be created during this load

### Visualizing flights

	java -cp target/thingspan-flights-1.0.0-SNAPSHOT.jar com.objectivity.thingspan.examples.flights.ThingSpanFlights -d data -b data/flights.boot -v

### Run using test data

	java -cp target/thingspan-flights-1.0.0-SNAPSHOT.jar com.objectivity.thingspan.examples.flights.ThingSpanFlights -d data -b data/flights.boot -v -t