import scala.io.Source
import java.util.Properties

object ProducerSample extends App {

  import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

  val  props = new Properties()
  props.put("bootstrap.servers", "10.188.193.161:6667")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val TOPIC="test_file"

  val filename = "data/data.dat"
//  val filename = args(0)
  var lineCount = 0
  for (line <- Source.fromFile(filename).getLines) {
    lineCount += 1
    val record = new ProducerRecord(TOPIC, lineCount.toString(), line  + "the end "+ new java.util.Date)
    print(record)
    producer.send(record)
  }



//  for(i<- 1 to 50){
//    val record = new ProducerRecord(TOPIC, "key", s"hello $i")
//    print(record)
//    producer.send(record)
//  }
//
//  val record = new ProducerRecord(TOPIC, "key", "the end "+new java.util.Date)
//  producer.send(record)

  producer.close()
}
