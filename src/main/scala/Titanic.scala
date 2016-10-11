import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by subhra on 1/9/16.
  */
object Titanic {

  val conf = new SparkConf().setAppName("Titanic")

  //create spark context object
  val sc = new SparkContext(conf)

  val titanic = sc.textFile("/datasets/TitanicData.txt")

  //PassengerId, Survived  (survived=0 & died=1), Pclass, Name, Sex
  //Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked

  // Average age of males and females who died in the Titanic tragedy

  val split = titanic.filter { x => {if(x.toString().split(",").length >= 6) true else false} }
                      .map(line=>{line.toString().split(",")})

  val key_value = split.filter{x=>if((x(1)=="1") && (x(5).matches(("\\d+"))))true else false}
                      .map(x => {(x(4),x(5).toInt)})

  key_value.mapValues((_, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
                    .mapValues{ case (sum, count) => (1.0 * sum)/count}.collectAsMap()


  // find the number of people who died or survived in each class

  val survive = titanic.filter { x => {if(x.toString().split(",").length >= 6) true else false} }
                      .map(line=>{line.toString().split(",")})

  val count=survive.map(x=>(x(1)+" "+x(4)+" "+x(6)+" "+x(2),1)).reduceByKey(_+_).collect

}
