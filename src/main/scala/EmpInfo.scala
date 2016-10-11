import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object EmpInfo {

  def main(args: Array[String]) {

    //create spark conf
    val conf = new SparkConf().setAppName("EmpInfo")

    //create spark context object
    val sc = new SparkContext(conf)

    // for dataframe
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //creating dataframe and joining two dataframes
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    val df = hc.sql("select * from spark.employee")

    //loading data from csv file
    var csvdata = sc.textFile("/datasets/empinfo.csv")
    var data = csvdata.map(line => line.split(",").map(elem =>elem.trim))
    val empcsv = data.map { case Array(s0, s1, s2) => emp(s0, s1, s2)}


    //converting into dataframe
    //val empdf = empcsv.toDF()
    val empdf = sqlContext.createDataFrame(empcsv)

    //joining operation
    val employeedf = empdf.join(df,empdf("empid")===df("id")).drop(df("id"))
      .select(
        empdf("empid"),
        empdf("designation"),
        empdf("unit"),
        df("expendeture"),
        df("start_date"),
        df("end_date")
      )

    //windows function
    val winfun = Window.partitionBy("empid").orderBy("empid","start_date").rowsBetween(-1, 0)
    val empwin = employeedf.withColumn( "date_expences",sum(employeedf("expendeture")).over(winfun))

    //saving into hive
    empwin.select($"empid",$"designation",$"unit",$"start_date",$"end_date",$"expendeture",$"date_expences")
      .write
      .format("orc")
      .saveAsTable("spark.empdetails1")

    sc.stop()
  }

}

case class emp(empid: String, designation: String, unit: String)


