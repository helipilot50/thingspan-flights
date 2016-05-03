name := "Flights"

version := "1.0"

scalaVersion := "2.10.6"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "3.0.0-M15" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" 

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.1"

libraryDependencies += "commons-cli" % "commons-cli" % "1.2"

libraryDependencies += "org.graphstream" % "gs-core" % "1.3"

libraryDependencies += "org.graphstream" % "gs-ui" % "1.3"

libraryDependencies += "org.jfree" % "jfreechart" % "1.0.19"

libraryDependencies += "org.scalanlp" % "breeze_2.10" % "0.12"
