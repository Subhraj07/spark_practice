package kafka.streaming


import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object SimpleConsumer {

 val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .getOrCreate()

  val sparkConf = new SparkConf().setAppName("KafkaSparkStreaming").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  case class Advertiser(advertiserId: Int,
                        advertiserName: String,
                        industry: String)

  val advertiserData = Array(Advertiser(0, "Hyundai", "Automobile"),
    Advertiser(1, "Toyota", "Automobile"),
    Advertiser(2, "Comcast", "Wireless"),
    Advertiser(3, "AT&T", "Wireless"),
    Advertiser(4, "Verizon", "Wireless"),
    Advertiser(5, "T-Mobile", "Wireless"),
    Advertiser(6, "Panasonic", "TV"),
    Advertiser(7, "Samsung", "TV"),
    Advertiser(8, "Coca-cola", "Soft Drinks"),
    Advertiser(9, "Pepsi-Co", "Soft Drinks"))

  import spark.implicits._

  val a = sc.parallelize(advertiserData)

  spark.createDataFrame(a).write.saveAsTable("AdvertiserInfo")

}
