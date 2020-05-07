import org.apache.spark.graphx.{EdgeTriplet, VertexId, _}
import org.apache.spark.sql._
import scala.reflect.ClassTag


object PatternQueryResolver
{
  def ResolveQuery[VD, ED:ClassTag](session: SparkSession, graph: Graph[VD, ED], query: Graph[VD => Boolean, ReachabilityQuery[VD, ED]]) : Graph[VertexId, Array[VertexPair]] =
  {
    //Initialize match sets
    val queryGraph = query.mapVertices((id, predicate) => graph.vertices.filter(data => predicate(data._2)).map(v => v._1).collect().toSet)
    //Just let pregel go and see what happens
    val results = Pregel.apply(queryGraph, Set[VertexId](), activeDirection = EdgeDirection.In)(vertexFunction, sendMessageFactory(graph, session), mergeMessage)
    results
      .mapTriplets(e => reachabilityQueryWithMatchSets(session, graph, e.attr.pathExpression, e.srcAttr, e.dstAttr))
      .mapVertices((id, data) => id)
  }

  def vertexFunction(id: VertexId, vertexState: Set[VertexId], searchMessage: Set[VertexId]): Set[VertexId] =
  {
    vertexState.diff(searchMessage)
  }

  def mergeMessage(left: Set[VertexId], right: Set[VertexId]) : Set[VertexId] =
  {
    left.union(right)
  }

  def sendMessageFactory[VD, ED:ClassTag](graph: Graph[VD, ED], session: SparkSession):
    EdgeTriplet[Set[VertexId], ReachabilityQuery[VD, ED]] => Iterator[(VertexId, Set[VertexId])] =
  {
    def sendMessage(edgeTriple: EdgeTriplet[Set[VertexId], ReachabilityQuery[VD, ED]]) : Iterator[(VertexId, Set[VertexId])] =
    {
      //Ridiculous nested Pregel
      val results = reachabilityQueryWithMatchSets(session, graph, edgeTriple.attr.pathExpression, edgeTriple.srcAttr, edgeTriple.dstAttr)
      val sourceRmv = edgeTriple.srcAttr.diff(results.map(pair => pair.source).toSet)

      if(sourceRmv.nonEmpty)
      {
        return Iterator((edgeTriple.dstId, sourceRmv))
      }
      else
      {
        return Iterator[(VertexId, Set[VertexId])]()
      }
    }

    sendMessage
  }

  def reachabilityQueryWithMatchSets[VD, ED:ClassTag](session: SparkSession, graph: Graph[VD, ED], query: Array[PathRegexTerm[ED]], matSource : Set[VertexId], matDest: Set[VertexId]) : Array[VertexPair] =
  {
    val newQuery = ReachabilityQuery[VD, ED](u1 => matSource(u1._1), u2 => matDest.contains(u2._1), query)
    ReachabilityParallelResolver.ResolveQuery(session, graph, newQuery)
  }

}
