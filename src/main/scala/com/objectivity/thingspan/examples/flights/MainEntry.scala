package com.objectivity.thingspan.examples.flights
import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser
import com.objectivity.thingspan.examples.flights.dataload.ReferenceData
import com.objectivity.thingspan.examples.flights.dataload.FlightsLoader
import com.objectivity.thingspan.examples.flights.visual.EasyVisual
import com.objectivity.thingspan.examples.flights.model.AppConfig

object MainEntry {
  	def main(args: Array[String]) = {
			var options = new Options()
			options.addOption("d", "data", true, "Data directory")
			options.addOption("b", "boot", true, "Boot file")
			options.addOption("r", "reference", false, "Load reference data")
			options.addOption("f", "flights", false, "Load flights data")
			options.addOption("v", "view", false, "View graph")

			val parser = new PosixParser()
			val cl = parser.parse(options, args, false)


			if (cl.hasOption("d")){
				val dataDirString = cl.getOptionValue("d", "data")
						AppConfig.DataDirectory = dataDirString
			}	

			if (cl.hasOption("b")){
				val bootString = cl.getOptionValue("b", "data/flights.boot")
						AppConfig.Boot = bootString
			}	
			if (cl.hasOption("v")){
			  EasyVisual.show()
			} else if (cl.hasOption("r")){
			  ReferenceData.load()
			} else 	if (cl.hasOption("f")){
			  FlightsLoader.load()
			} else {
			  println("options:")
			  println("\t-d path to data directory - optional [data]")
			  println("\t-b boot file - optional [data/flights.boot]")
			  println("\t-r load reference data")
			  println("\t-f load flights data")
			  println("\t-v view graph")
			}
  	}
}