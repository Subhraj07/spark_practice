import java.io._
import java.util.Properties

import KafkaFileProducerDemo.{args, _}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object Basic extends App {

  val TOPIC = "test_file"
  val filename = args(0)

  val props = new Properties
  props.put("bootstrap.servers", "10.188.193.161:6667")
  props.put("client.id", "KafkaFileProducerDemo")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  var producer = new KafkaProducer[String, String](props)

  var lineCount = 0
  val files = getListOfFiles(args(0))
  for (f <- files) {
    var fis = new FileInputStream(f)
    var br: Option[BufferedReader] = None
    br = Some(new BufferedReader(new InputStreamReader(fis)))
    var br1: BufferedReader = br.get

    try {
      var line = ""

      while (br1.readLine != null) {
        line = br1.readLine
        lineCount += 1
        val record = new ProducerRecord(TOPIC, lineCount.toString, line + "the end " + new java.util.Date)
        System.out.println("Sent message: (" + lineCount + ", " + line + ")")
        //      println("Sent message: (" + lineCount + ", " + line + ")")
        //      print(record)
        producer.send(record)
        Thread.sleep(100)

      }

    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally
      try br1.close()
      catch {
        case e: IOException => e.printStackTrace()
      }
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }

  }
}

