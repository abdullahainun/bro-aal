name := "bro-dns-streaming"
version := "0.1"
scalaVersion := "2.11.6"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.2"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.2"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.3.2"
libraryDependencies += "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.3.2"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.2"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.2"
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.5.0"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.0.0"

assemblyMergeStrategy in assembly := {
  {
    case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}
