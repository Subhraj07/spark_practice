import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.io.BufferedReader
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStreamReader
import java.util.Properties
import java.util.concurrent.ExecutionException

//import ProducerEx.props
import org.apache.kafka.clients.producer.RecordMetadata

object KafkaStreamProducer {
  val topicName = "test_log"
  val fileName = "/home/dev/subhrajit/data/logs.txt"

  def main(args: Array[String]): Unit = {
    val producer = new KafkaFileProducer(topicName, false)
    var lineCount = 0
    var fis = new FileInputStream(fileName)
    var br:Option[BufferedReader] = None
    br = Some(new BufferedReader(new InputStreamReader(fis)))
    var br1: BufferedReader = br.get

    try {
      //Construct BufferedReader from InputStreamReader
      var line = ""

      while ( br1.readLine != null )
      {
        line = br1.readLine
        lineCount += 1
        producer.sendMessage(lineCount + "", line)
      }
    } catch {
      case e: Exception =>
        // TODO Auto-generated catch block
        e.printStackTrace()
    } finally try br1.close()
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }
}

class KafkaFileProducer(val topic: String, val isAsync: Boolean) extends Thread {
  val props = new Properties
  props.put("bootstrap.servers", "10.188.193.161:6667")
  props.put("client.id", "DemoProducer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  var producer = new KafkaProducer[String, String](props)
  def sendMessage(key: String, value: String): Unit = {
    val startTime = System.currentTimeMillis
    if (isAsync) { // Send asynchronously
      producer.send(new ProducerRecord[String, String](topic, key), new DemoCallBack(startTime, key, value).asInstanceOf[Callback])
    }
    else { // Send synchronously
      try {
        producer.send(new ProducerRecord[String, String](topic, key, value)).get
        System.out.println("Sent message: (" + key + ", " + value + ")")
      } catch {
        case e: InterruptedException =>
          e.printStackTrace()
        case e: ExecutionException =>
          e.printStackTrace()
      }
    }
  }
}

class DemoCallBack(var startTime: Long, var key: String, var message: String) extends Callback {
  /**
    * A callback method the user can implement to provide asynchronous handling
    * of request completion. This method will be called when the record sent to
    * the server has been acknowledged. Exactly one of the arguments will be
    * non-null.
    *
    * @param metadata
    * The metadata for the record that was sent (i.e. the partition
    * and offset). Null if an error occurred.
    * @param exception
    * The exception thrown during processing of this record. Null if
    * no error occurred.
    */
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    val elapsedTime = System.currentTimeMillis - startTime
    if (metadata != null) System.out.println("message(" + key + ", " + message + ") sent to partition(" + metadata.partition + "), " + "offset(" + metadata.offset + ") in " + elapsedTime + " ms")
    else exception.printStackTrace()
  }
}