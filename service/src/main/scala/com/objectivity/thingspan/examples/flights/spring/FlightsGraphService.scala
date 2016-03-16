package com.objectivity.thingspan.examples.flights.spring

import java.io.PrintWriter
import java.io.StringWriter

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.context.web.SpringBootServletInitializer
import org.springframework.context.annotation.Bean

import com.objectivity.thingspan.examples.flights.AppConfig


@SpringBootApplication
class FlightsGraphService extends SpringBootServletInitializer{

	override def  configure(application: SpringApplicationBuilder ) : SpringApplicationBuilder = {
			application.sources(classOf[FlightsGraphService]);
	}

}


object FlightsGraphService {
	def main(args: Array[String])  {
	  var options = new Options();
		options.addOption("d", "data", true, "Data directory");
		options.addOption("t", "test", false, "Use test data");
		options.addOption("b", "boot", true, "Boot file");
		options.addOption("u", "usage", false, "Print usage.");

		val parser = new PosixParser()
		val cl = parser.parse(options, args, false)

		if (cl.hasOption("u")) {
			val formatter = new HelpFormatter();
			var sw = new StringWriter();
			var pw = new PrintWriter(sw);
			val syntax = FlightsGraphService.getClass().getName() + " [<options>]";
			formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
			println(sw.toString());
			return
		}

    if (cl.hasOption("d")){
			val dataDirString = cl.getOptionValue("d", "data")
			AppConfig.DataDirectory = dataDirString
		}	
    
    if (cl.hasOption("t"))
		    AppConfig.TestData = true
			SpringApplication.run(classOf[FlightsGraphService])
			
		if (cl.hasOption("b")){
				val bootString = cl.getOptionValue("b", "data/flights.boot")
						AppConfig.Boot = bootString
		}	

	}

	@Bean
	def sparkContext() : SparkContext = {
			var conf = new SparkConf()
			conf.setAppName("FlightGraphService")
			conf.setMaster("local[1]")
			new SparkContext(conf)
	}
	

}