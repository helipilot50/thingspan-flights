# US Domestic Flights

## Install Thingspan jar
Install the thingspan jar to a local repo - only once
	
	mvn deploy:deploy-file -Durl=file://repo -Dfile=lib/spark-rdd-1.0.0-all.jar -DgroupId=com.objectivity -DartifactId=thingspan-spark-rdd -Dpackaging=jar -Dversion=1.0.0

## Loading Data

	java -cp dataload/target/thingspan-flights-dataload-1.0.0.jar com.objectivity.thingspan.examples.flights.FlightsLoader 
	
	sudo java -Djava.library.path=/home/peter/ThingSpan/15.0/lib -cp thingspan-flights-dataload-1.0.0.jar com.objectivity.thingspan.examples.flights.FlightsLoader -d data -b ~/data/flights.boot
	
	sudo java -Djava.library.path=/home/peter/ThingSpan/15.0/lib -cp thingspan-flights-dataload-1.0.0.jar com.objectivity.thingspan.examples.flights.FlightsLoader -r -b ~/data/flights.boot
	
or

	java -jar dataload/target/thingspan-flights-dataload-1.0.0.jar


