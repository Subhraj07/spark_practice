package kafka.streaming

import kafka.streaming.CheckPointSpark.checkpointDir
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object WindowSpark extends App{

  val sparkConf = new SparkConf().setAppName("KafkaSparkStreaming").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(5))


  ssc.checkpoint(checkpointDir)

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "10.188.193.161:6667",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  //  val topics = Array("topicA", "topicB")
  val topics = args(0)
  val topicsSet = topics.split(",").toList
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent, // distribute partitions evenly across available executors
    Subscribe[String, String](topicsSet, kafkaParams)
  )

  var key = stream.map(record=>(record.key()))
  var key1 = stream.map(x=>x.value()).filter(_.contains("NASA")).window(Seconds(25),Seconds(10))

  //var count = value.reduceByKey(_+_)
  var filterRecs = key1.filter(y=>y.contains("NASA")).print()

//  var filterRecs = stream.map(x=>x.value()).window(Seconds(60),Seconds(30))

  //  println("Records - " +key.print()+"--------------------"+count.print())
  println("Filtered Records : "+filterRecs)

  ssc.start()
  ssc.awaitTermination()

}
