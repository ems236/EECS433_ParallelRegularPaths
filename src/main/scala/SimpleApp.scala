import org.apache.spark.graphx.Pregel
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

object SimpleApp {
  def main(args: Array[String]) {

    println(s"STARTING")


    val logFile = "/opt/spark-2.4.5-bin-hadoop2.7/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    val graph = BitcoinDataLoader.bitcoinGraph(spark)
    println(graph.vertices.count())
    println(graph.edges.count())


    //Send some reachability queries
    //Should return all edges where the trust is worse than 3
    val testRegex = new PathRegexTerm[BitcoinEdgeAttribute](e => e.trust == -5, Some(1))
    val testQuery = ReachabilityQuery[Int, BitcoinEdgeAttribute](_ => true, _ => true, Array(testRegex))

    val results = ReachabilitySequentialResolver.ResolveQuery(spark, graph, testQuery)
    val results2 = ReachabilityParallelResolver.ResolveQuery(spark, graph, testQuery)

    println(results.length)
    println(results2.length)
    //results.sortBy(v => v.source).foreach(v => println(s"(${v.source}, ${v.dest})"))

    spark.stop()
  }
}