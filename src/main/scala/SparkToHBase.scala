package spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer

object SparkOnHbase extends App {

  val sparkConf = new SparkConf().setAppName("Logs").setMaster("local[*]")

  sparkConf.set("spark.cores.max", "16")
  sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
  sparkConf.set("spark.sql.tungsten.enabled", "true")
  //    sparkConf.set("spark.eventLog.enabled", "true")
  sparkConf.set("spark.app.id", "Logs")
  sparkConf.set("spark.io.compression.codec", "snappy")
  sparkConf.set("spark.rdd.compress", "false")
  sparkConf.set("spark.suffle.compress", "true")

  val sc = new SparkContext(sparkConf)

  /*
  create 'employee', 'emp_personal_data', 'emp_professional_data'

  put 'employee','1','emp_personal_data:name','govardhan'
  put 'employee','1','emp_personal_data:city','kurnool'
  put 'employee','1','emp_professional_data:designation','manager'
  put 'employee','1','emp_professional_data:salary','30000'

  scan 'employee'

  spark-shell --master yarn-client --driver-class-path=/usr/hdp/2.6.1.0-129/hbase/lib/hbase-common.jar:/usr/hdp/2.6.1.0-129/hbase/lib/hbase-client.jar:/usr/hdp/2.6.1.0-129/hbase/lib/hbase-protocol.jar:/usr/hdp/2.6.1.0-129/hbase/lib/hbase-server.jar:/etc/hbase/conf:/usr/hdp/2.6.1.0-129/oozie/share/lib/spark/guava-14.0.1.jar
   */
  val conf: Configuration = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "10.188.193.161")
  conf.set("hbase.zookeeper.property.clientPort", "2181")

  val connection: Connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(TableName.valueOf("employee"))
  print("connection created")

  for (rowKey <- 1 to 1) {
    val result = table.get(new Get(Bytes.toBytes(rowKey.toString())))
    val nameDetails = result.getValue(Bytes.toBytes("emp_personal_data"), Bytes.toBytes("name"))
    val cityDetails = result.getValue(Bytes.toBytes("emp_personal_data"), Bytes.toBytes("city"))
    val designationDetails = result.getValue(Bytes.toBytes("emp_professional_data"), Bytes.toBytes("designation"))
    val salaryDetails = result.getValue(Bytes.toBytes("emp_professional_data"), Bytes.toBytes("salary"))
    val name = Bytes.toString(nameDetails)
    val city = Bytes.toString(cityDetails)
    val designation = Bytes.toString(designationDetails)
    val salary = Bytes.toString(salaryDetails)
    println("Name is " + name + ", city " + city + ", Designation " + designation + ", Salary " + salary)
  }

}
