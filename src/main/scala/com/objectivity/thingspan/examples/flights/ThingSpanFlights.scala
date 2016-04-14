package com.objectivity.thingspan.examples.flights
import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser
import com.objectivity.thingspan.examples.flights.dataload.ReferenceData
import com.objectivity.thingspan.examples.flights.dataload.FlightsLoader
import com.objectivity.thingspan.examples.flights.visual.EasyVisual
import com.objectivity.thingspan.examples.flights.model.AppConfig
import org.apache.commons.cli.HelpFormatter

object ThingSpanFlights {
  	def main(args: Array[String]) = {
			var options = new Options()
			options.addOption("d", "data", true, "Data directory")
			options.addOption("b", "boot", true, "Boot file")
			options.addOption("r", "reference", false, "Load reference data")
			options.addOption("f", "flights", false, "Load flights data")
			options.addOption("v", "view", false, "View graph")
			options.addOption("t", "test", false, "Test data")
			options.addOption("m", "master", true, "Spark Master default: local[*]")

			val parser = new PosixParser()
			val cl = parser.parse(options, args, false)


			if (cl.hasOption("t")){
				AppConfig.TestData = true
			}	else {
			  AppConfig.TestData = false
			}
			if (cl.hasOption("d")){
				val dataDirString = cl.getOptionValue("d", "data")
						AppConfig.DataDirectory = dataDirString
			}	

			if (cl.hasOption("b")){
				val bootString = cl.getOptionValue("b", "data/flights.boot")
						AppConfig.Boot = bootString
			}	

			if (cl.hasOption("m")){
				val masterString = cl.getOptionValue("m", "local[*]")
						AppConfig.SparkMaster = masterString
			}	

			if (cl.hasOption("v")){
			  EasyVisual.show()
			} else if (cl.hasOption("r")){
			  ReferenceData.load()
			} else 	if (cl.hasOption("f")){
			  FlightsLoader.load()
			} else {
			  var formatter = new HelpFormatter();
			  usage(formatter, options, 0)
			}
  	}
  	
  	def usage(formatter: HelpFormatter, options: Options, 
           exitCode: Int) { 
       formatter.printHelp("java -jar thingspan-flights-<version>.jar", options) 
       System.exit(exitCode) 
   } 
}