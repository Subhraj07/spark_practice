package kafka.streaming


import org.apache.spark.streaming.{ StreamingContext, Seconds }
import org.apache.spark.SparkConf

object WindowTest {

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext(new SparkConf().setAppName("TestAdditionJob"), Seconds(5))

    val msg = ssc.socketTextStream("10.188.193.161", 8899)

    msg
      .map(data => ("sum", data.toInt))
      .reduceByKey(_ + _)
      .window(Seconds(10), Seconds(5))
      .print()

    ssc.start()
    ssc.awaitTermination()
  }

}
