# US Domestic Flights

## Install Thingspan jar
Install the tingspan jar to tat local repo - only once
	
	mvn deploy:deploy-file -Durl=file://repo -Dfile=lib/spark-rdd-1.0.0-all.jar -DgroupId=com.objectivity -DartifactId=thingspan-spark-rdd -Dpackaging=jar -Dversion=1.0.0

## Loading Data

	java -cp dataload/target/thingspan-flights-dataload-1.0.0.jar com.objectivity.thingspan.examples.flights.FlightsLoader 
	
or

	java -jar dataload/target/thingspan-flights-dataload-1.0.0.jar


