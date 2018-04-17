
import org.apache.spark.SparkContext
import scala.util.matching.Regex
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.graphx._


object GraphT {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: User<input file> <output file>")
      System.exit(1);
    }

    val conf = new SparkConf().setAppName("Inverse") setMaster ("local[2]")
    val sc = new SparkContext(conf)

    val higgsData = sc.textFile(args(0)).map(l => l.split(" "))

    val edges = higgsData.map(l => (l(0).toLong, l(1).toLong))

    val graphEdges = Graph.fromEdgeTuples(edges, 1)

    val TotalNodes = graphEdges.numVertices

    val inDegree = graphEdges.inDegrees

    val nkDegree = inDegree.map(x => (x._2, 1)).reduceByKey((x, y) => (x + y))
    // val mapInDegress= inDegree.map(line=>line._1->line._2)
    //val nkDegree = inDegree.map(x => (x._2)).countByValue

    val finalRdd = nkDegree.map(x => (x._1, x._2.toFloat / TotalNodes))

    finalRdd.saveAsTextFile(args(1))

    sc.stop();

  }
}