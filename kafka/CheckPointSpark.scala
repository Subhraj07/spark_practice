package kafka.streaming

import kafka.streaming.WindowSpark.kafkaParams
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object CheckPointSpark {

  val checkpointDir: String = "/tmp/dev/subhrajit/checkpoint1/"

  val topics = "test_file"
  val topicsSet = topics.split(",").toList

  def main(args: Array[String]): Unit = {

    val ssc = StreamingContext.getOrCreate(checkpointDir, createFunc)

    ssc.start()
    ssc.awaitTermination()
  }

  def createFunc(): StreamingContext = {
    val ssc = new StreamingContext(new SparkConf().setAppName("TestMapWithStateJob"),
      Seconds(5))

    ssc.checkpoint(checkpointDir)

    // State specs
    val stateSpec = StateSpec.function(mappingFunc _)
      .numPartitions(4)
      .timeout(Seconds(30)) // idle keys will be removed.

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent, // distribute partitions evenly across available executors
      Subscribe[String, String](topicsSet, kafkaParams)
    )

//      stream.map(x=>x.value()).flatMap(_.split(" "))
//      .map((_, 1))
//      .mapWithState(stateSpec)
//      .checkpoint(Seconds(20))
//      .print()

    stream.map(record=>(record.value().toString)).print()

    ssc
  }

  def mappingFunc(key: String, value: Option[Int], state: State[Int]): Option[(String, Int)] = {
    val sum = value.getOrElse(0) + state.getOption().getOrElse(0)

    // updating the state of non-idle keys...
    // To call State.update(...) we need to check State.isTimingOut() == false,
    // else there will be NoSuchElementException("Cannot update the state that is timing out")
    if (state.isTimingOut())
      println(key + " key is timing out...will be removed.")
    else
      state.update(sum)

    Some((key, sum))
  }
}

