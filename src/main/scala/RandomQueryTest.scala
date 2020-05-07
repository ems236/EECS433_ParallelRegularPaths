import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.util.Random

object RandomQueryTest
{
  def main(args: Array[String]) {
    val SEED = 125413

    val spark = SparkSession.builder.appName("Parallel Reachability").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val graph = BitcoinDataLoader.bitcoinGraph(spark)

    val rand = new Random(SEED)
    val domain = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)


    var totalParallelTime = 0L
    var totalResults = 0
    for(_ <- Range(0, 10))
    {
      val reachabilityInfo = QueryGenerator.ReachabilityQueryFor(4, 2, domain, rand)
      println(reachabilityInfo)
      val reachabilityRegex = reachabilityInfo.map(v => new PathRegexTerm[BitcoinEdgeAttribute](b => b.trust == v._1, Some(v._2))).toArray
      val query = ReachabilityQuery[Int, BitcoinEdgeAttribute](v => true, v => true, reachabilityRegex)


      //Dont even bother with sequential
      //var start = System.currentTimeMillis()
      //val results = ReachabilitySequentialResolver.ResolveQuery(spark, graph, query)
      //var stop = System.currentTimeMillis()
      //println(s"Sequential time ${stop - start}, length ${results.size}")
      //results.sortBy(v => v.source).foreach(v => println(s"(${v.source}, ${v.dest})"))

      val start = System.currentTimeMillis()
      val results2 = ReachabilityParallelResolver.ResolveQuery(spark, graph, query)
      val stop = System.currentTimeMillis()
      println(s"Parallel time ${stop - start}, length ${results2.size}")

      totalParallelTime = totalParallelTime + stop - start
      totalResults = totalResults + results2.size

      //val r1 = spark.sparkContext.parallelize(results)
      //val r2 = spark.sparkContext.parallelize(results2)

      //println(s"diff 2 - 1 = ${r2.subtract(r1).count()}")
      //println(s"diff 1 - 2 = ${r1.subtract(r2).count()}")
    }

    println(s"Time: ${totalParallelTime / 10}")
    println(s"Results: ${totalResults / 10}")
    spark.stop()
  }
}
