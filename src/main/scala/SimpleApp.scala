import org.apache.spark.graphx.{Edge, Pregel, _}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object SimpleApp {
  def main(args: Array[String]) {

    println(s"STARTING")


    val logFile = "/opt/spark-2.4.5-bin-hadoop2.7/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext

    spark.sparkContext.setLogLevel("WARN")
    /*
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

    */

    val users: RDD[(VertexId, String)] =
      sc.parallelize(Array((1L, "B"), (2L, "B")
        , (3L, "C"), (4L, "C"), (5L, "C")
        , (6L, "D")
        , (7L, "H")))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(
        Edge(1L, 2L, "fa"), Edge(1L, 7L, "fa"), Edge(1L, 6L, "fn"),
        Edge(2L, 1L, "fa"), Edge(2L, 7L, "fa"), Edge(2L, 6L, "fn"),
        Edge(3L, 1L, "fa"), Edge(3L, 4L, "fa"), Edge(3L, 6L, "sa"),
        Edge(4L, 3L, "fa"), Edge(4L, 5L, "fa"),
        Edge(5L, 1L, "fn"), Edge(5L, 2L, "fn"), Edge(5L, 3L, "fa"),
        Edge(7L, 5L, "sn")
      ))

    // Define a default user in case there are relationship with missing user
    val defaultUser = "F"
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    println(graph.vertices.count())


    val querydata: Array[(VertexId, String => Boolean)] =
      Array(
        (1L, s => s == "B"),
        (2L, s => s == "C")
        , (3L, s => s == "D"))
    val queryNodes = sc.parallelize(querydata)

    val fnRegex = Array(new PathRegexTerm[String](s => s == "fn", Some(1)))
    val faRegex = Array(new PathRegexTerm[String](s => s == "fa", Some(3)))
    val faSnRegex = Array(new PathRegexTerm[String](s => s == "fa", Some(2))
      , new PathRegexTerm[String](s => s == "sn", Some(1)))
    val faSaRegex = Array(new PathRegexTerm[String](s => s == "fa", Some(2))
      , new PathRegexTerm[String](s => s == "sa", Some(2)))

    val queryEdgeData: Array[Edge[Array[PathRegexTerm[String]]]] =
      Array(
        Edge(1L, 2L, faSnRegex),
        Edge(1L, 3L, fnRegex),
        Edge(2L, 1L, fnRegex),
        Edge(2L, 2L, faRegex),
        Edge(2L, 3L, faSaRegex)
      )
    // Create an RDD for edges
    val queryEdges = sc.parallelize(queryEdgeData)
    // Define a default user in case there are relationship with missing user
    val defaultVal : String => Boolean = s => true
    // Build the initial Graph
    val query = Graph(queryNodes, queryEdges, defaultVal)

    val results = PatternQueryResolver.ResolveQuery(spark, graph, query)
    results.edges.foreach(e => println(e))

    spark.stop()
  }
}