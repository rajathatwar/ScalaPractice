import org.apache.spark.SparkContext
import scala.util.matching.Regex
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Comp {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: User<input file> <output file>")
      System.exit(1);
    }
    val conf = new SparkConf().setAppName("Inverse")setMaster("local[2]")
    val sc = new SparkContext(conf)

   
    
    val higgsData=sc.textFile(args(0)).map(l=>l.split(" "))
    
    //val higgsData=sc.textFile("hdfs:///hadoop-user/data/higgs-social_network.edgelist").map(l=>l.split(" "))

    val higgsKeyPair1=higgsData.map(l=>(l(0),l(1)))
    
    val higgsKeyPair2=higgsData.map(l=>(l(1),l(0)))

    val lfJoinData=higgsKeyPair1.leftOuterJoin(higgsKeyPair2).map{case (a, (b, c: Option[Int]))=>(a,(b,(c.getOrElse())))}
    
    val filteredRdd=lfJoinData.filter{case (key,(val1,val2))=>(val2!=val1)}
    
    val finalData=filteredRdd.map(l=>(l._1,l._2._1)).distinct()
   // finalData.saveAsTextFile("/hadoop-user/data/alloutput/higgs")
    finalData.saveAsTextFile(args(1));
    sc.stop()
     

    
}
}