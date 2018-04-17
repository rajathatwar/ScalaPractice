import org.apache.spark.SparkContext
import scala.util.matching.Regex
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib._
import org.apache.spark.mllib.recommendation._

object userArtistData {
  def adSum(ad: Array[Double]): Double = {
    var sum = 0.0
    var i = 0
    while (i < ad.length) { sum += ad(i); i += 1 }
    sum
  }

  def getRank(actual: Array[(Int, Double)], predicted: Array[(Int, Double)]): Double = {
  
    var total: Double = 0
    val actRatArray = actual.map(_._2)
    predicted.map(l => total += ((l._2 * (predicted.indexOf(l) + 1) / predicted.size)).toDouble / adSum(actRatArray))
    total
  }
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: User<input file> <output file>")
      System.exit(1);
    }

    val conf = new SparkConf().setAppName("UserArtist") setMaster ("local[2]")
    val sc = new SparkContext(conf)

    val artistData = sc.textFile(args(0))

    val artistAlias = sc.textFile(args(1))

    val a = artistAlias.flatMap { l => val t = l.split("\t"); if (t(0).isEmpty) { None } else { Some((t(0).toInt, t(1).toInt)) } }.collectAsMap

    val broadcastAlias = sc.broadcast(a)

    val CleanData = artistData.map { l => val Array(uID, aId, pCount) = l.split(" ").map(l => l.toInt); val FilterArtistId = broadcastAlias.value.getOrElse(aId, aId); Rating(uID, FilterArtistId, pCount) }

    val Array(training, testing) = CleanData.randomSplit(Array(0.8, 0.2))
    val rank = 10; val lambda = 0.01; val numIterations = 20; val alpha = 0.1
    val model = ALS.trainImplicit(training, rank, numIterations, lambda, alpha)

    val userproducts = testing.map { case Rating(uId, aId, pCount) => (uId, aId) }
    val predictions = model.predict(userproducts).map { case Rating(uId, aId, pCount) => (uId, (aId, pCount)) }
    val sorted_pred = predictions.sortBy(-_._2._2)

    val actual = testing.map { case Rating(uId, aId, pCount) => (uId, (aId, pCount)) }
    val joined = actual.join(sorted_pred)
    joined.take(3).foreach(println)

    val rankedUsers = joined.map(l => (l._1, getRank(Array(l._2._1), Array(l._2._2))))
    rankedUsers.take(3).foreach(println)
   
    val finalAverage = rankedUsers.map(_._2).reduce(_ + _) / rankedUsers.count()
    println("average is: " + finalAverage)

    sc.stop()

  }
}