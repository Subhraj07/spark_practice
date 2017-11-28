//package win.check
//
//import kafka.streaming.CheckPointSpark.checkpointDir
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.sql.{SparkSession,SQLContext}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
//import win.check.Spark_Streaming.args
//
//object Spark_Sensor_Stream extends App{
//
//  val spark = SparkSession
//    .builder()
//    .appName("Spark SQL basic example")
//    .getOrCreate()
//
//  val sparkConf = new SparkConf().setAppName("KafkaSparkStreaming").setMaster("local[*]")
//  val sc = new SparkContext(sparkConf)
//  val ssc = new StreamingContext(sc, Seconds(5))
//
//
//  ssc.checkpoint(checkpointDir)
//
//  val kafkaParams = Map[String, Object](
//    "bootstrap.servers" -> "10.188.193.161:6667",
//    "key.deserializer" -> classOf[StringDeserializer],
//    "value.deserializer" -> classOf[StringDeserializer],
//    "group.id" -> "use_a_separate_group_id_for_each_stream",
//    "auto.offset.reset" -> "latest",
//    "enable.auto.commit" -> (false: java.lang.Boolean)
//  )
//
//  //  val topics = Array("topicA", "topicB")
//  val topics = args(0)
//  val topicsSet = topics.split(",").toList
//  val stream = KafkaUtils.createDirectStream[String, String](
//    ssc,
//    PreferConsistent, // distribute partitions evenly across available executors
//    Subscribe[String, String](topicsSet, kafkaParams)
//  )
//
//    stream.foreachRDD { rdd =>
//
//      // There exists at least one element in RDD
//      if (!rdd.isEmpty) {
//        val count = rdd.count
//        println("count received " + count)
//        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
//        import sqlContext.implicits._
//        import org.apache.spark.sql.functions._
//
//        val sensorDF = rdd.toDF()
//        // Display the top 20 rows of DataFrame
//        println("sensor data")
//        sensorDF.show()
//        sensorDF.registerTempTable("sensor")
//        val res = sqlContext.sql("SELECT resid, date, count(resid) as total FROM sensor GROUP BY resid, date")
//        println("sensor count ")
//        res.show
//        val res2 = sqlContext.sql("SELECT resid, date, avg(psi) as avgpsi FROM sensor GROUP BY resid,date")
//        println("sensor psi average")
//        res2.show
//
//      }
//    }
//  // Start the computation
//  println("start streaming")
//
//
//  ssc.start()
//  ssc.awaitTermination()
//
//
//}
