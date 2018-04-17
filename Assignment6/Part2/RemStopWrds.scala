import org.apache.spark.SparkContext
import scala.util.matching.Regex
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object RemStopWrds {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: User<input file> <output file>")
      System.exit(1);
    }
    val conf = new SparkConf().setAppName("Inverse") setMaster ("local[2]")
    val sc = new SparkContext(conf)

    val book = sc.textFile(args(0)).map(l => l.split("\\W+")).map(l => l.map(_.toLowerCase))
    val stopWordList = sc.textFile(args(1)).flatMap(l => l.split(",")).map(_.trim)
    val broadCastStopWords = sc.broadcast(stopWordList.collect.toSet)
    val bookWithoutStopWords = book.map(word => word.filter(!broadCastStopWords.value.contains(_)).mkString(" "))
    bookWithoutStopWords.repartition(1).saveAsTextFile(args(2))
    sc.stop()
  }
}