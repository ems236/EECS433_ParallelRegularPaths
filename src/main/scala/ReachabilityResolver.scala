import org.apache.spark.graphx._

case class VertexPair(source: VertexId, dest: VertexId)
case class ReachabilityQuery[VD, ED](sourceFilter: ((VertexId, VD)) => Boolean,
                                     destFilter: ((VertexId, VD)) => Boolean,
                                     pathExpression: List[PathRegexTerm[ED]])

class VertexStatus[VD]()

object ReachabilityResolver
{
  def ResolveQuery[VD, ED](graph: Graph[VD, ED], reachabilityQuery: ReachabilityQuery[VD, ED]) : List[VertexPair] =
  {
    //Bi-directional search
    val source_set = graph.vertices.filter(reachabilityQuery.sourceFilter)
    val dest_set = graph.vertices.filter(reachabilityQuery.destFilter)
  }

  def
}
