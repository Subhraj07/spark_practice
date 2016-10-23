/**
  * Created by subhra on 23/10/16.
  */

import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
object BasketBallAnalysis {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("NBA_processing").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // for dataframe
    val sqlContext = new HiveContext(sc)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //read in all stats
    val stats=sc.textFile("/user/ambari/prep/processed/BasketballStatsWithYear/*/*").repartition(sc.defaultParallelism)

    //filter out junk rows, clean up data entry errors as well
    val filteredStats=stats.filter(line => !line.contains("FG%")).filter(line => line.contains(",")).map(line => line.replace("*","").replace(",,",",0,"))
    filteredStats.cache()

    //process stats and save as map
    val txtStat = Array("FG","FGA","FG%","3P","3PA","3P%","2P","2PA","2P%","eFG%","FT","FTA","FT%","ORB","DRB","TRB","AST","STL","BLK","TOV","PF","PTS")
    val aggStats = processStats(filteredStats,txtStat).collectAsMap

    //collect rdd into map and broadcast
    val broadcastStats = sc.broadcast(aggStats)

    //parse stats, now tracking weights
    val txtStatZ = Array("FG","FT","3P","TRB","AST","STL","BLK","TOV","PTS")
    val zStats = processStats(filteredStats,txtStatZ,broadcastStats.value).collectAsMap

    //collect rdd into map and broadcast
    val zBroadcastStats = sc.broadcast(zStats)

    //parse stats, now normalizing
    val nStats = filteredStats.map(line => bbParse(line,broadcastStats.value,zBroadcastStats.value))

    //create schema for the data frame
    val schemaN =StructType(
      StructField("name", StringType, true) ::
        StructField("year", IntegerType, true) ::
        StructField("age", IntegerType, true) ::
        StructField("position", StringType, true) ::
        StructField("team", StringType, true) ::
        StructField("gp", IntegerType, true) ::
        StructField("gs", IntegerType, true) ::
        StructField("mp", DoubleType, true) ::
        StructField("FG", DoubleType, true) ::
        StructField("FGA", DoubleType, true) ::
        StructField("FGP", DoubleType, true) ::
        StructField("3P", DoubleType, true) ::
        StructField("3PA", DoubleType, true) ::
        StructField("3PP", DoubleType, true) ::
        StructField("2P", DoubleType, true) ::
        StructField("2PA", DoubleType, true) ::
        StructField("2PP", DoubleType, true) ::
        StructField("eFG", DoubleType, true) ::
        StructField("FT", DoubleType, true) ::
        StructField("FTA", DoubleType, true) ::
        StructField("FTP", DoubleType, true) ::
        StructField("ORB", DoubleType, true) ::
        StructField("DRB", DoubleType, true) ::
        StructField("TRB", DoubleType, true) ::
        StructField("AST", DoubleType, true) ::
        StructField("STL", DoubleType, true) ::
        StructField("BLK", DoubleType, true) ::
        StructField("TOV", DoubleType, true) ::
        StructField("PF", DoubleType, true) ::
        StructField("PTS", DoubleType, true) ::
        StructField("zFG", DoubleType, true) ::
        StructField("zFT", DoubleType, true) ::
        StructField("z3P", DoubleType, true) ::
        StructField("zTRB", DoubleType, true) ::
        StructField("zAST", DoubleType, true) ::
        StructField("zSTL", DoubleType, true) ::
        StructField("zBLK", DoubleType, true) ::
        StructField("zTOV", DoubleType, true) ::
        StructField("zPTS", DoubleType, true) ::
        StructField("zTOT", DoubleType, true) ::
        StructField("nFG", DoubleType, true) ::
        StructField("nFT", DoubleType, true) ::
        StructField("n3P", DoubleType, true) ::
        StructField("nTRB", DoubleType, true) ::
        StructField("nAST", DoubleType, true) ::
        StructField("nSTL", DoubleType, true) ::
        StructField("nBLK", DoubleType, true) ::
        StructField("nTOV", DoubleType, true) ::
        StructField("nPTS", DoubleType, true) ::
        StructField("nTOT", DoubleType, true) :: Nil
    )

    //map RDD to RDD[Row] so that we can turn it into a DataFrame
    val nPlayer = nStats.map(x => Row.fromSeq(Array(x.name,x.year,x.age,x.position,x.team,x.gp,x.gs,x.mp) ++ x.stats ++ x.statsZ ++ Array(x.valueZ) ++ x.statsN ++ Array(x.valueN)))

    //create data frame
    val dfPlayersT = sqlContext.createDataFrame(nPlayer,schemaN)

    //save all stats as a temp table
    dfPlayersT.registerTempTable("tPlayers")

    //calculate experience levels
    val dfPlayers=sqlContext.sql("select age-min_age as exp,tPlayers.* from tPlayers join (select name,min(age)as min_age from tPlayers group by name) as t1 on tPlayers.name=t1.name order by tPlayers.name, exp ")

    //save as table
    dfPlayers.write.format("orc").saveAsTable("Players")

    // Programmatic Data Analysis with Spark SQL
    //    dfPlayers.groupBy("age").count.sort("age").show(100)
    //    sqlContext.sql("Select name, zTot from Players where year=2016 order by zTot desc").take(10).foreach(println)
    //    sqlContext.sql("Select name, nTot from Players where year=2016 order by nTot desc").take(10).foreach(println)
    //    sqlContext.sql("Select * from Players where year=2016 and name='Stephen Curry'").collect.foreach(println)
    //    sqlContext.sql("select name, 3p, z3p from Players  where year=2016 order by z3p desc").take(10).foreach(println)
    //    sqlContext.sql("select name, 3p, z3p from Players order by 3p desc").take(10).foreach(println)
    //    sqlContext.sql("select name, 3p, z3p from Players order by z3p desc").take(10).foreach(println)

    sc.stop()
  }

  def statNormalize(stat:Double, max:Double, min:Double)={
    val newmax=math.max(math.abs(max),math.abs(min))
    stat/newmax
  }

  /*A function that takes a line of input from the Basketball-Reference.com data, computes new statistics,
  and returns a BballData object. This allows us to do all the computations described in the appendix.*/
  def bbParse(input: String,bStats: scala.collection.Map[String,Double]=Map.empty,zStats: scala.collection.Map[String,Double]=Map.empty)={
    val line=input.replace(",,",",0,")
    val pieces=line.substring(1,line.length-1).split(",")
    val year=pieces(0).toInt
    val name=pieces(2)
    val position=pieces(3)
    val age=pieces(4).toInt
    val team=pieces(5)
    val gp=pieces(6).toInt
    val gs=pieces(7).toInt
    val mp=pieces(8).toDouble
    val stats=pieces.slice(9,31).map(x=>x.toDouble)
    var statsZ:Array[Double]=Array.empty
    var valueZ:Double=Double.NaN
    var statsN:Array[Double]=Array.empty
    var valueN:Double=Double.NaN

    if (!bStats.isEmpty){
      val fg=(stats(2)-bStats.apply(year.toString+"_FG%_avg"))*stats(1)
      val tp=(stats(3)-bStats.apply(year.toString+"_3P_avg"))/bStats.apply(year.toString+"_3P_stdev")
      val ft=(stats(12)-bStats.apply(year.toString+"_FT%_avg"))*stats(11)
      val trb=(stats(15)-bStats.apply(year.toString+"_TRB_avg"))/bStats.apply(year.toString+"_TRB_stdev")
      val ast=(stats(16)-bStats.apply(year.toString+"_AST_avg"))/bStats.apply(year.toString+"_AST_stdev")
      val stl=(stats(17)-bStats.apply(year.toString+"_STL_avg"))/bStats.apply(year.toString+"_STL_stdev")
      val blk=(stats(18)-bStats.apply(year.toString+"_BLK_avg"))/bStats.apply(year.toString+"_BLK_stdev")
      val tov=(stats(19)-bStats.apply(year.toString+"_TOV_avg"))/bStats.apply(year.toString+"_TOV_stdev")*(-1)
      val pts=(stats(21)-bStats.apply(year.toString+"_PTS_avg"))/bStats.apply(year.toString+"_PTS_stdev")
      statsZ=Array(fg,ft,tp,trb,ast,stl,blk,tov,pts)
      valueZ = statsZ.reduce(_+_)

      if (!zStats.isEmpty){
        val zfg=(fg-zStats.apply(year.toString+"_FG_avg"))/zStats.apply(year.toString+"_FG_stdev")
        val zft=(ft-zStats.apply(year.toString+"_FT_avg"))/zStats.apply(year.toString+"_FT_stdev")
        val fgN=statNormalize(zfg,(zStats.apply(year.toString+"_FG_max")-zStats.apply(year.toString+"_FG_avg"))/zStats.apply(year.toString+"_FG_stdev"),(zStats.apply(year.toString+"_FG_min")-zStats.apply(year.toString+"_FG_avg"))/zStats.apply(year.toString+"_FG_stdev"))
        val ftN=statNormalize(zft,(zStats.apply(year.toString+"_FT_max")-zStats.apply(year.toString+"_FT_avg"))/zStats.apply(year.toString+"_FT_stdev"),(zStats.apply(year.toString+"_FT_min")-zStats.apply(year.toString+"_FT_avg"))/zStats.apply(year.toString+"_FT_stdev"))
        val tpN=statNormalize(tp,zStats.apply(year.toString+"_3P_max"),zStats.apply(year.toString+"_3P_min"))
        val trbN=statNormalize(trb,zStats.apply(year.toString+"_TRB_max"),zStats.apply(year.toString+"_TRB_min"))
        val astN=statNormalize(ast,zStats.apply(year.toString+"_AST_max"),zStats.apply(year.toString+"_AST_min"))
        val stlN=statNormalize(stl,zStats.apply(year.toString+"_STL_max"),zStats.apply(year.toString+"_STL_min"))
        val blkN=statNormalize(blk,zStats.apply(year.toString+"_BLK_max"),zStats.apply(year.toString+"_BLK_min"))
        val tovN=statNormalize(tov,zStats.apply(year.toString+"_TOV_max"),zStats.apply(year.toString+"_TOV_min"))
        val ptsN=statNormalize(pts,zStats.apply(year.toString+"_PTS_max"),zStats.apply(year.toString+"_PTS_min"))
        statsZ=Array(zfg,zft,tp,trb,ast,stl,blk,tov,pts)
        valueZ = statsZ.reduce(_+_)
        statsN=Array(fgN,ftN,tpN,trbN,astN,stlN,blkN,tovN,ptsN)
        valueN=statsN.reduce(_+_)
      }
    }
    BballData(year, name, position, age, team, gp, gs, mp, stats,statsZ,valueZ,statsN,valueN)
  }

  /*takes in the raw data and calculates our mean, max, min, and standard deviation values.
  This function takes an RDD of Strings, which are the raw lines of statistical data, parses them
   */
  //process raw data into zScores and nScores
  def processStats(stats0:org.apache.spark.rdd.RDD[String],txtStat:Array[String],bStats: scala.collection.Map[String,Double]=Map.empty,zStats: scala.collection.Map[String,Double]=Map.empty)={
    //parse stats
    val stats1=stats0.map(x=>bbParse(x,bStats,zStats))

    //group by year
    val stats2={if(bStats.isEmpty){
      stats1.keyBy(x=>x.year).map(x=>(x._1,x._2.stats)).groupByKey()
    }else{
      stats1.keyBy(x=>x.year).map(x=>(x._1,x._2.statsZ)).groupByKey()
    }
    }

    //map each stat to StatCounter
    val stats3=stats2.map{case (x,y)=>(x,y.map(a=>a.map(b=>BballStatCounter(b))))}

    //merge all stats together
    val stats4=stats3.map{case (x,y)=>(x,y.reduce((a,b)=>a.zip(b).map{ case (c,d)=>c.merge(d)}))}

    //combine stats with label and pull label out
    val stats5=stats4.map{case (x,y)=>(x,txtStat.zip(y))}.map{x=>(x._2.map{case (y,z)=>(x._1,y,z)})}

    //separate each stat onto its own line and print out the Stats to a String
    val stats6=stats5.flatMap(x=>x.map(y=>(y._1,y._2,y._3.printStats(","))))

    //turn stat tuple into key-value pairs with corresponding agg stat
    val stats7=stats6.flatMap{case(a,b,c)=>{
      val pieces=c.split(",")
      val count=pieces(0)
      val mean=pieces(1)
      val stdev=pieces(2)
      val max=pieces(3)
      val min=pieces(4)
      Array((a+"_"+b+"_"+"count",count.toDouble),(a+"_"+b+"_"+"avg",mean.toDouble),(a+"_"+b+"_"+"stdev",stdev.toDouble),(a+"_"+b+"_"+"max",max.toDouble),(a+"_"+b+"_"+"min",min.toDouble))
    }
    }
    stats7
  }

  //process stats for age or experience
  def processStatsAgeOrExperience(stats0:org.apache.spark.rdd.RDD[(Int, Array[Double])], label:String)={


    //group elements by age
    val stats1=stats0.groupByKey()

    //turn values into StatCounter objects
    val stats2=stats1.map{case(x,y)=>(x,y.map(z=>z.map(a=>BballStatCounter(a))))}

    //Reduce rows by merging StatCounter objects
    val stats3=stats2.map{case (x,y)=>(x,y.reduce((a,b)=>a.zip(b).map{case(c,d)=>c.merge(d)}))}

    //turn data into RDD[Row] object for dataframe
    val stats4=stats3.map(x=>Array(Array(x._1.toDouble),x._2.flatMap(y=>y.printStats(",").split(",")).map(y=>y.toDouble)).flatMap(y=>y)).map(x=>Row(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),x(20)))

    //create schema for age table
    val schema =StructType(
      StructField(label, IntegerType, true) ::
        StructField("valueZ_count", DoubleType, true) ::
        StructField("valueZ_mean", DoubleType, true) ::
        StructField("valueZ_stdev", DoubleType, true) ::
        StructField("valueZ_max", DoubleType, true) ::
        StructField("valueZ_min", DoubleType, true) ::
        StructField("valueN_count", DoubleType, true) ::
        StructField("valueN_mean", DoubleType, true) ::
        StructField("valueN_stdev", DoubleType, true) ::
        StructField("valueN_max", DoubleType, true) ::
        StructField("valueN_min", DoubleType, true) ::
        StructField("deltaZ_count", DoubleType, true) ::
        StructField("deltaZ_mean", DoubleType, true) ::
        StructField("deltaZ_stdev", DoubleType, true) ::
        StructField("deltaZ_max", DoubleType, true) ::
        StructField("deltaZ_min", DoubleType, true) ::
        StructField("deltaN_count", DoubleType, true) ::
        StructField("deltaN_mean", DoubleType, true) ::
        StructField("deltaN_stdev", DoubleType, true) ::
        StructField("deltaN_max", DoubleType, true) ::
        StructField("deltaN_min", DoubleType, true) :: Nil
    )

    //create data frame
    //sqlContext.createDataFrame(stats4,schema)
  }

}

/*A class that holds the statistics as received from Basketball-Reference.com,
as well as arrays and doubles that will correspond to our z-scores and normalized z-scores as we process the data*/
@serializable case class BballData(year: Int, name: String, position: String, age:Int, team: String, gp: Int, gs: Int,
                                   mp: Double,stats: Array[Double], statsZ:Array[Double]=Array[Double](),
                                   valueZ:Double=0,statsN:Array[Double]=Array[Double](),valueN:Double=0,experience:Double=0)



