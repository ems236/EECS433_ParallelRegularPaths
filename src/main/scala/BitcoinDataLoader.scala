import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._

import org.apache.spark.graphx.Pregel
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

object BitcoinDataLoader {
  val PATH = "/home/ellis/shareddata/School/databases/project/regular_path_queries/src/soc-sign-bitcoinotc.csv"

  val SOURCE_KEY = "source_id"
  val DEST_KEY = "dest_id"
  val TRUST_KEY = "value"
  val TIME_KEY = "time"

  def bitcoinDataFrame(session: SparkSession): DataFrame = {
    val schema = new StructType()
      .add(SOURCE_KEY, IntegerType, nullable = false)
      .add(DEST_KEY, IntegerType, nullable = false)
      .add(TRUST_KEY, IntegerType, nullable = false)
      .add(TIME_KEY, DoubleType, nullable = false)

    val test = session.read.p
    val df = session.read.format("com.databricks.spark.csv")
      .schema(schema)
      .option("delimiter", ",")
      .load(PATH)

    return df
  }

  def bitcoinGraph(session: SparkSession) : Graph[Int, Int] =
  {
    import session.sqlContext.implicits._

    val df = bitcoinDataFrame(session)
    var edge_data = df.map(row => Edge(
      row.getAs[Int](SOURCE_KEY),
      row.getAs[Int](DEST_KEY),
      attr = BitcoinEdgeAttribute(row.getAs[Int](TRUST_KEY), row.getAs[Double](TIME_KEY))
    )
    )

    val edges : EdgeRDD[BitcoinEdgeAttribute] = EdgeRDD.fromEdges(edge_data.rdd)

    val graph: Graph[Int, BitcoinEdgeAttribute] = Graph.fromEdges(edges, defaultValue = 0)

    return graph
  }
}