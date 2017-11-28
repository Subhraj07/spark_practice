import Sample.args
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object Kafka10Spark extends App {

  val sparkConf = new SparkConf().setAppName("KafkaSparkStreaming").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(10))

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

  stream.map(record => (record.key.toString, " value " , record.value.toString)).print()

  ssc.start()
  ssc.awaitTermination()

}
