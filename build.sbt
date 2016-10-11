name := "spark_xml"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq (
  "org.apache.spark" % "spark-core_2.10" % "1.6.0",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.0",
  "org.apache.spark" % "spark-hive_2.10" % "1.6.0",
  "org.apache.spark" % "spark-yarn_2.10" % "1.6.0",
  "com.databricks" % "spark-xml_2.10" % "0.2.0")