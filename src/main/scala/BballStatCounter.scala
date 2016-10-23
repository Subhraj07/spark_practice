/**
  * Created by subhra on 23/10/16.
  */

import org.apache.spark.util.StatCounter

//stat counter class -- need printStats method to print out the stats. Useful for transformations
/* A new object that we use to calculate aggregate statistics across a dataset.
contains an instance of Spark’s StatCounter class, which allows us to easily calculate things
like mean, max, min, and standard deviation across a data set*/
object BballStatCounter extends Serializable {
  def apply(x: Double) = new BballStatCounter().add(x)
}
class BballStatCounter extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double): BballStatCounter = {
    if (x.isNaN) {
      missing += 1
    } else {
      stats.merge(x)
    }
    this
  }

  def merge(other: BballStatCounter): BballStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }

  def printStats(delim: String): String= {
    stats.count + delim + stats.mean + delim + stats.stdev + delim + stats.max + delim + stats.min
  }

  override def toString: String = {
    "stats: " + stats.toString + " NaN: " + missing
  }
}


