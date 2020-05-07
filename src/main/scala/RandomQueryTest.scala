import org.apache.spark.sql.SparkSession
import scala.util.Random

object RandomQueryTest
{
  def main(args: Array[String]) {
    val SEED = 1235413

    val spark = SparkSession.builder.appName("Parallel Reachability").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val graph = BitcoinDataLoader.bitcoinGraph(spark)

    val rand = new Random(SEED)
    val domain = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val reachabilityInfo = QueryGenerator.ReachabilityQueryFor(5, 3, domain, rand)
    println(reachabilityInfo)
    val reachabilityRegex = reachabilityInfo.map(v => new PathRegexTerm[BitcoinEdgeAttribute](b => b.trust == v._1, Some(v._2))).toArray

    val query = ReachabilityQuery[Int, BitcoinEdgeAttribute](v => true, v => true, reachabilityRegex)

    var start = System.currentTimeMillis()
    var results = ReachabilitySequentialResolver.ResolveQuery(spark, graph, query)
    var stop = System.currentTimeMillis()
    println(s"Sequential time ${stop - start}, length ${results.size}")
    //results.sortBy(v => v.source).foreach(v => println(s"(${v.source}, ${v.dest})"))

    start = System.currentTimeMillis()
    results = ReachabilityParallelResolver.ResolveQuery(spark, graph, query)
    stop = System.currentTimeMillis()
    println(s"Parallel time ${stop - start}, length ${results.size}")
    //results.sortBy(v => v.source).foreach(v => println(s"(${v.source}, ${v.dest})"))
    spark.stop()
  }
}
