import org.apache.spark.SparkContext
import scala.util.matching.Regex
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class Movies(mId: Int, gen: String)
case class Ratings(uId: Int, mId: Int, rat: Double)

object MvRtng {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: User<input file> <output file>")
      System.exit(1);
    }
    

    val spark = SparkSession.builder().master("local").appName("Spark SQL basic").config("spark.sql.warehouse.dir", "/home/administrator/warehouse").getOrCreate
   // val conf=new SparkConf().setAppName("Test")
   // val sc=new SparkContext(conf)
    val sc=spark.sparkContext
    //val sqlContext =new org.apache.spark.sql.SQLContext(sc)
   // import sqlContext.implicits._
    import spark.implicits._
    

    val movies = sc.textFile(args(0)).map(l => l.split('#')).map(l => (l(0).toInt, l(2))).flatMapValues(g => g.split('|')).map(l => Movies(l._1, l._2)).toDF

    

    val rating = sc.textFile(args(1)).map(l => l.split('#')).map(l => Ratings(l(0).toInt, l(1).toInt, l(2).toDouble)).toDF

    rating.createOrReplaceTempView("rating_view")

    movies.createOrReplaceTempView("movies_view")

    val average = spark.sql("select ra.uId, mv.gen, avg(ra.rat) as avg_rating from movies_view mv, rating_view ra where mv.mId=ra.mId group by ra.uId,mv.gen")

    val uIdSort = average.sort($"uId".asc)
    uIdSort.createOrReplaceTempView("uIdSort_view")

    val rankDF = spark.sql("select u.uId, u.gen, u.avg_rating, RANK() OVER (partition by u.uId order by u.avg_rating DESC) AS rank FROM uidsort_view u")

    rankDF.createOrReplaceTempView("rankedProducts")

    val finalData = spark.sql("select uId, gen, avg_rating from rankedProducts where rank<=5")

    finalData.rdd.saveAsTextFile(args(2));
    sc.stop()
    
  }
}