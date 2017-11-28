import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object Sample extends App {

  val sparkConf = new SparkConf().setAppName("KafkaSparkStreaming").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(10))
//  val topics = "spark-test-topic"
  val topics = args(0)
  val topicsSet = topics.split(",").toSet


  val kafkaConf = Map(
    //        "metadata.broker.list" -> "10.188.193.161:6667",
    "bootstrap.servers" -> "10.188.193.161:6667",
    "zookeeper.connect" -> "10.188.193.161:2181",
    "group.id" -> "kafka-streaming-example",
    "enable.auto.commit" -> "true",
    "auto.commit.interval.ms" -> "1000",
    "session.timeout.ms" -> "30000",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    // "partition.assignment.strategy" -> "range"
    "zookeeper.connection.timeout.ms" -> "1000"
  )

  val kafkaStream = createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topicsSet, kafkaConf))
  kafkaStream.map(record=>(record.value().toString)).print()
  kafkaStream.map(record=>(record.offset().toString)).print()
  kafkaStream.map(record=>(record.offset().toString)).print()

  ssc.start()
  ssc.awaitTermination()
}
