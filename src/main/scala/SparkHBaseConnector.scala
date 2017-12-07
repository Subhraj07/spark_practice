package spark.hbase

import it.nerdammer.spark.hbase._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}


object SparkHBaseConnector extends App {

  val APP_NAME: String = "SparkHbaseJob"
  var HBASE_DB_HOST: String = null
  var HBASE_TABLE: String = null
  var HBASE_COLUMN_FAMILY: String = null
  var HIVE_DATA_WAREHOUSE: String = null


  HBASE_DB_HOST="10.188.193.161"
  HBASE_TABLE="driver_dangerous_event"
  HBASE_COLUMN_FAMILY="events"
  // Initializing Hive Metastore configuration
  HIVE_DATA_WAREHOUSE = "/usr/local/hive/warehouse"

  // setting spark application
  val sparkConf = new SparkConf().setAppName("Logs").setMaster("local[*]")
  //initialize the spark context
  val sc = new SparkContext(sparkConf)
  //Configuring Hbase host with Spark/Hadoop Configuration
  sc.hadoopConfiguration.set("spark.hbase.host", HBASE_DB_HOST)
  // Read HBase Table
  val hBaseRDD = sc.hbaseTable[(Option[String], Option[String], Option[String], Option[String], Option[String],Option[String], Option[String], Option[String], Option[String])](HBASE_TABLE).select("driverId", "driverName", "eventTime", "eventType", "latitudeColumn","longitudeColumn","routeId","routeName","truckId").inColumnFamily(HBASE_COLUMN_FAMILY)
  // Iterate HBaseRDD and generate RDD[Row]
  val rowRDD = hBaseRDD.map(i => Row(i._1.get,i._2.get,i._3.get,i._4.get,i._5.get,i._6.get,i._7.get,i._8.get,i._9.get))
  // Create sqlContext for createDataFrame method
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)


  object empSchema {
    val driverId = StructField("driverId", StringType)
    val driverName = StructField("driverName", StringType)
    val eventTime = StructField("eventTime", StringType)
    val eventType = StructField("eventType", StringType)
    val latitudeColumn = StructField("latitudeColumn", StringType)
    val longitudeColumn = StructField("longitudeColumn", StringType)
    val routeId = StructField("routeId", StringType)
    val routeName = StructField("routeName", StringType)
    val truckId = StructField("truckId", StringType)
    val struct = StructType(Array(driverId, driverName, eventTime, eventType, latitudeColumn,longitudeColumn,routeId,routeName,truckId))
  }
//
  // Create DataFrame with rowRDD and Schema structure
  val stdDf = sqlContext.createDataFrame(rowRDD,empSchema.struct);
  stdDf.show()
  // Enable Hive with Hive warehouse in SparkSession
//  val spark = SparkSession.builder().appName(APP_NAME).config("spark.sql.warehouse.dir", HIVE_DATA_WAREHOUSE).enableHiveSupport().getOrCreate()
//  // Importing spark implicits and sql package
//  import spark.implicits._
//  import spark.sql
//
//  // Saving Dataframe to Hive Table Successfully.
//  stdDf.write.mode("append").saveAsTable("employee")

}

/*
create a table
--------------
create 'driver_dangerous_event','events'

list of tables
--------------
list

show data
---------
scan 'driver_dangerous_event'

inser data
----------
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=,  -Dimporttsv.columns="HBASE_ROW_KEY,events:driverId,events:driverName,events:eventTime,events:eventType,events:latitudeColumn,events:longitudeColumn,events:routeId,events:routeName,events:truckId" driver_dangerous_event /user/dev/subhrajit/data.csv


insert the records to the table
-------------------------------
put 'driver_dangerous_event','4','events:driverId','78'
put 'driver_dangerous_event','4','events:driverName','Carl'
put 'driver_dangerous_event','4','events:eventTime','2016-09-23 03:25:03.567'
put 'driver_dangerous_event','4','events:eventType','Normal'
put 'driver_dangerous_event','4','events:latitudeColumn','37.484938'
put 'driver_dangerous_event','4','events:longitudeColumn','-119.966284'
put 'driver_dangerous_event','4','events:routeId','845'
put 'driver_dangerous_event','4','events:routeName','Santa Clara to San Diego'
put 'driver_dangerous_event','4','events:truckId','637'


update the current record value
-------------------------------
put 'driver_dangerous_event','4','events:routeName','Santa Clara to Los Angeles'


Get perticular value
--------------------
get 'driver_dangerous_event','1'

get 'driver_dangerous_event','1',{COLUMN => 'events:driverName'}

get 'driver_dangerous_event','1',{COLUMNS => ['events:driverName','events:routeId']}

*/

/****************************************/
/*    loading into hbase                */
/***************************************/
/* https://github.com/nerdammer/spark-hbase-connector */
/*
create 'mytable','mycf'
val rdd = sc.parallelize(1 to 100).map(i => (i.toString, i+1, "Hello"))
rdd.toHBaseTable("mytable").toColumns("column1", "column2").inColumnFamily("mycf").save()


*/
