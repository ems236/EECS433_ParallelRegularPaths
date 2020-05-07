import org.apache.spark.graphx.{EdgeTriplet, VertexId, _}
import org.apache.spark.sql._

import scala.collection.mutable
import scala.reflect.ClassTag

object PatternQueryResolver
{
  def ResolveQuery[VD, ED](graph: Graph[VD, ED], query: Graph[VD => Boolean, ReachabilityQuery[VD, ED]]) : Graph[VD, Array[VertexPair]] =
  {
        
  }
}
