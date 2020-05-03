import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.Pregel
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}



object BitcoinDataLoader {
  val PATH = "/home/ellis/shareddata/School/databases/project/regular_path_queries/src/soc-sign-bitcoinotc.csv"

  def main(args: Array[String])
  {
    val spark = SparkSession.builder.appName("Graph Load").getOrCreate()

    println("Initialized")

    val schema = new StructType()
      .add("source_id",IntegerType,false)
      .add("dest_id",IntegerType,false)
      .add("value",IntegerType,false)
      .add("time",DoubleType,false)

    val df = spark.read.format("com.databricks.spark.csv")
      .schema(schema)
      .option("delimiter", ",")
      .load(PATH)

    println("Loaded " + df.count())

    spark.stop()

  }
}