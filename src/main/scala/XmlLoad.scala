import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by subhra on 30/8/16.
  */
object XmlLoad {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("EmpInfo")

    //create spark context object
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //creating dataframe and joining two dataframes
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    val df = hc.read.format("com.databricks.spark.xml").option("rowTag","project").load("/datasets/pom.xml")
    val df1 = hc.read.format("com.databricks.spark.xml").option("rowTag","tradedata").load("/datasets/data.xml")
    df.registerTempTable("xmldf")
    df1.registerTempTable("xmldf1")
    hc.sql("drop table if exists spark.xmldata")
    hc.sql("drop table if exists spark.xmldata1")
    val xmldfhive = hc.sql("select * from xmldf")
    val xmldf1hive = hc.sql("select * from xmldf1")
    xmldfhive.saveAsTable("spark.xmldata")
    xmldf1hive.saveAsTable("spark.xmldata1")
    sc.stop()
  }

}
