import org.apache.spark.graphx._

case class VertexPair(source: VertexId, dest: VertexId)
case class ReachabilityQuery[VD, ED](sourceFilter: ((VertexId, VD)) => Boolean,
                                     destFilter: ((VertexId, VD)) => Boolean,
                                     pathExpression: List[PathRegexTerm[ED]])

case class TermStatus(isFrontier: Boolean, maxForwardIndex: Int, minBackwardsIndex: Int, originIds: List[VertexId])

object ReachabilityResolver
{
  def ResolveQuery[VD, ED](graph: Graph[VD, ED], reachabilityQuery: ReachabilityQuery[VD, ED]) : List[VertexId] =
  {
    //Bi-directional search
    val sources = graph.vertices.filter(reachabilityQuery.sourceFilter)
    val dests = graph.vertices.filter(reachabilityQuery.destFilter)

    val lastIndex = reachabilityQuery.pathExpression.length - 1
    val source_set = VertexRDD[TermStatus](sources.map(v => (v._1, TermStatus(true, 0, lastIndex + 1, List(v._1)))))
    val dest_set = VertexRDD[TermStatus](dests.map(v => (v._1, TermStatus(true, -1, lastIndex, List(v._1)))))

  }

  def expandedFrontier[VD, ED](graph: Graph[VD, ED], vertexSet: VertexRDD[TermStatus]) : VertexRDD[TermStatus] =
  {
    
  }
}
