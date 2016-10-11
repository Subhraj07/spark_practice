import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by subhra on 31/8/16.
  */
object UberProcess {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("UberProcess")

    //create spark context object
    val sc = new SparkContext(conf)

    val dataset = sc.textFile("/datasets/uber")
    val header = dataset.first()
    val format = new java.text.SimpleDateFormat("MM/dd/yyyy")
    var days =Array("Sun","Mon","Tue","Wed","Thu","Fri","Sat")
    val eliminate = dataset.filter(line => line != header)
    val split = eliminate.map(line => line.split(",")).map { x => (x(0),format.parse(x(1)),x(3)) }
    val combine = split.map(x => (x._1+" "+days(x._2.getDay),x._3.toInt))
    val arrange = combine.reduceByKey(_+_).map(item => item.swap).sortByKey(false).collect.foreach(println)
    sc.stop()
  }

}
