name := "adms_streaming"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"
libraryDependencies += "org.apache.commons" % "commons-compress" % "1.8.1"
libraryDependencies +=   "org.apache.spark" %% "spark-core" % "2.2.1"
libraryDependencies +=   "org.apache.spark" %% "spark-sql" % "2.2.1"
libraryDependencies +=   "org.apache.spark" %% "spark-streaming" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1"
libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.10.1.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.0.0"
libraryDependencies += "org.postgresql" % "postgresql" % "42.1.4"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"
