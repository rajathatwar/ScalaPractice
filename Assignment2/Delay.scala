
import org.apache.spark.SparkContext
import scala.util.matching.Regex
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Delay {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: Delay<input file> <output file>")
      System.exit(1);
    }
    val conf = new SparkConf().setAppName("MDelayCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val flightRdd = sc.textFile("file:///home/administrator/ScalaLab/Assignment4/2001.csv")

    val header = flightRdd.first()
    val flightDataRdd = flightRdd.filter(lines => lines != header).map(line => line.split(","))

    val splitData = flightDataRdd.map(splits => (splits(8), splits(16), splits(17), splits(0) + "/" + splits(1) + "/" + splits(2), splits(15).replaceAll("NA", "0").toInt))
    val finalData = splitData.map(line => line).sortBy(-_._5)
    //finalData.saveAsTextFile(args(1))
    finalData.take(10).foreach(println)

    sc.stop()
  }

}