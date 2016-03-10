package com.objectivity.thingspan.examples.flights.spring

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.SpringApplication
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.apache.commons.cli.PosixParser
import java.io.PrintWriter
import java.io.StringWriter
import com.objectivity.thingspan.examples.flights.FlightsLoader
import com.objectivity.thingspan.examples.flights.CreateRelationships
import org.springframework.boot.context.web.SpringBootServletInitializer
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.annotation.Bean
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.CreatableRelationProvider
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
		options.addOption("l", "load", false, "Load flight data");
		options.addOption("r", "relation", false, "Create relationships");
		options.addOption("t", "test", false, "Use test data");
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

		if (cl.hasOption("l")){
			FlightsLoader.main(args)
		} else if (cl.hasOption("r")){
			CreateRelationships.main(args)
		} else {
		  if (cl.hasOption("t"))
		    AppConfig.TestData = true
			SpringApplication.run(classOf[FlightsGraphService])
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