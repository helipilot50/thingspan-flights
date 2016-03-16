# US Domestic Flights

## Install Thingspan jar
Install the thingspan jar to a local repo - only once
	
	mvn deploy:deploy-file -Durl=file://repo -Dfile=lib/spark-rdd-1.0.0-all.jar -DgroupId=com.objectivity -DartifactId=thingspan-spark-rdd -Dpackaging=jar -Dversion=1.0.0

## Loading Data
```bash
	spark-submit \
  	--master local[*] \
  	--class com.objectivity.thingspan.examples.flights.FlightsLoader \
	--total-executor-cores 10 \
	--executor-memory 10g \
	thingspan-flights-dataload-1.0.0.jar -d data -b flights.boot
```
## Creating replationships
```bash
	spark-submit \
  	--master Local[1] \
  	--class com.objectivity.thingspan.examples.flights.FlightsLoader \
	--total-executor-cores 1 \
	--executor-memory 10g \
	thingspan-flights-dataload-1.0.0.jar -r -b flights.boot
```


