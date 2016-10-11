import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by subhra on 1/9/16.
  */
object TravelAnalysis {

  val conf = new SparkConf().setAppName("TravelAnalysis")

  //create spark context object
  val sc = new SparkContext(conf)

  val traveldata = sc.textFile("/datasets/TravelData.txt")

  //City pair (Combination of from and to): String,From location: String, To Location: String, Product type: Integer
  //  (1=Air, 2=Car, 3 =Air+Car, 4 =Hotel, 5=Air+Hotel, 6=Hotel +Car, 7 =Air+Hotel+Car)
  //Adults traveling: Integer, Seniors traveling: Integer,Children traveling: Integer, Youth traveling: Integer,
  //Infant traveling: Integer,Date of travel: String, Time of travel: String,Date of Return: String,
  // Time of Return: String, Price of booking: Float, Hotel name: String

  // Problem statement:  Top 20 destination people travel the most
  val dest = traveldata.map(lines=>lines.split('\t')).map(x=>(x(2),1)).reduceByKey(_+_)
                    .map(item => item.swap).sortByKey(false).take(20)

  // Problem statement:  Top 20 locations from where people travel the most
  val from = traveldata.map(lines=>lines.split('\t')).map(x=>(x(1),1)).reduceByKey(_+_)
    .map(item => item.swap).sortByKey(false).take(20)

  // Problem statement:  Top 20 cities that generate high airline revenues for travel
  val airline = traveldata.map(x=>x.split('\t')).filter(x=>{if((x(3).matches(("1")))) true else false })
                  .map(x=>(x(2),1)).reduceByKey(_+_).map(item => item.swap).sortByKey(false).take(20)






}
