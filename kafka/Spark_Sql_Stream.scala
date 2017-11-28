package win.check

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}

object Spark_Sql_Stream extends App{

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .getOrCreate()

  val kafka = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "10.188.193.161:6667")   // comma separated list of broker:host
    .option("subscribe", "test_file")    // comma separated list of topics
    .option("startingOffsets", "latest") // read data from the end of the stream
    .load()

//  val df = kafka.select(explode(split(value.toString(), "\\s+")).as("word"))
//    .groupBy("word")
//    .count
//
//  df.select($"word", $"count")


}
