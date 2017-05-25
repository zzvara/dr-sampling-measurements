name := "dr-sampling-measurements"

version := "1.0"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  Resolver.defaultLocal,
  "Jzy3d Releases" at "http://maven.jzy3d.org/releases")

libraryDependencies += "hu.sztaki" % "dynamic-repartitioning-core_2.11" % "0.1.34-SNAPSHOT"

libraryDependencies += "default" % "freq-count_2.11" % "1.0"

libraryDependencies += "com.github.tototoshi" % "scala-csv_2.11" % "1.3.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"

libraryDependencies += "org.scalanlp" % "breeze_2.11" % "0.13"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.5.1"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.5.1"

libraryDependencies += "org.jzy3d" % "jzy3d-api" % "1.0.0" from "http://maven.jzy3d.org/releases/org/jzy3d/jzy3d-api/1.0.0/jzy3d-api-1.0.0.jar"

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.5"
)