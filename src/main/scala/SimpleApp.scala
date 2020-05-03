import org.apache.spark.graphx.Pregel
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

object SimpleApp {
  def main(args: Array[String]) {

    println(s"STARTING")


    val logFile = "/opt/spark-2.4.5-bin-hadoop2.7/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    val graph = BitcoinDataLoader.bitcoinGraph(spark)
    println(graph.vertices.count())
    println(graph.edges.count())

    spark.stop()
  }
}