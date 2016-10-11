import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by subhra on 1/9/16.
  */
object Youtube {

  val conf = new SparkConf().setAppName("Youtube")

  //create spark context object
  val sc = new SparkContext(conf)

  val youtube = sc.textFile("/datasets/youtubedata.txt")

  //Video id of 11 characters, Uploader of the video,
  //Interval between day of establishment of YouTube and the date of uploading of the video.
  //Category of the video, Length of the video, Number of views for the video, Rating on the video
  //Number of ratings given for the video, Number of comments on the videos, Related video ids with the uploaded video.

  // what are the top five categories with maximum number of videos uploaded.
  val maxnumvdo = youtube.map(line=>{var YoutubeRecord = ""; val temp=line.split("\t");
              if(temp.length >= 3) {YoutubeRecord=temp(3)};YoutubeRecord})

  val topfive=maxnumvdo.map (x => (x,1)).reduceByKey(_+_).map(item=>item.swap).sortByKey(false).take(5)

  // Top 10 rated videos in YouTube
  val maxrat = youtube.filter { x => {if(x.toString().split("\t").length >= 6) true else false} }.
              map(line=>{line.toString().split("\t")})


  val rating = maxrat.map(line=>(line(0),line(6).toDouble)).reduceByKey(_+_).map(line=>line.swap).sortByKey(false).take(10)



}
