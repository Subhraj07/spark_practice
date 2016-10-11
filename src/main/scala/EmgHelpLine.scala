import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by subhra on 11/9/16.
  */
object EmgHelpLine {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("EmergencyHelpLine")
    val sc= new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits

    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    import hc.implicits

    val data = sc.textFile(args(0))
    val header = data.first()

    val emgdata = data.filter(h => h!=header)
    val emergency_data = emgdata.map(x=>x.split(",")).filter(x => x.length>=9)
      .map(x => emergency(x(0),x(1),x(2),x(3),x(4).substring(0,x(4).indexOf(":")),x(5),x(6),x(7),x(8)))

    val df = sqlContext.createDataFrame(emergency_data)
    df.show
  }

}

case class emergency(lat:String,lng:String,desc:String,zip:String,title:String,timeStamp:String,twp:String,addr:String,e:String)
