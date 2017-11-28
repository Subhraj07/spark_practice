package win.check

import kafka.streaming.CheckPointSpark.checkpointDir
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object Spark_Streaming extends App{

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

  stream.foreachRDD { rdd =>

    val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    println("offsets -> " + offsets)
    for(offsetRange <- offsets)
    {
      val offset_range = offsetRange.untilOffset - offsetRange.fromOffset
      println("offset ranges of an rdd " + offset_range)
    }

    val collected = rdd.mapPartitionsWithIndex { (i, iter) =>

      //partition index of an rdd, to get the offset range for a partition in an rdd.
      val osr: OffsetRange = offsets(i)

      // getting info from the received offset
      val topic = osr.topic
      println("topic -> " + topic)
      val kafkaPartitionId = osr.partition
      println("kafkaPartitionId -> " + kafkaPartitionId)
      val begin = osr.fromOffset
      println("begin -> " + begin)
      val end = osr.untilOffset
      println("end -> " + end)
      Iterator(topic,kafkaPartitionId,begin,end)
    }.collect
  }

//  print("Values -> ")
//  val savestream = stream.map(x=>(x.value().toString)).saveAsTextFiles("/tmp/dev/subhrajit/data/sample.txt")

  stream.map(record=> "Topic of " + (record.topic().toString) + " has value " + (record.value().toString)).print()

//  println()

  ssc.start()
  ssc.awaitTermination()

}
