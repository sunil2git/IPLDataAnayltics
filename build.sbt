name := "testPro"

version := "0.1"
scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "mysql" % "mysql-connector-java" % "5.1.6",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0",
  "net.liftweb" %% "lift-json" % "3.4.1",

 //for Kafka
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-streaming" % "2.3.0" % "provided",
  "org.apache.kafka" % "kafka-clients" % "2.3.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.0",

  "commons-logging" % "commons-logging" % "1.1.1"
)
