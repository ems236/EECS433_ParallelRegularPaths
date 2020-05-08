import org.apache.spark.graphx.{EdgeTriplet, VertexId, _}
import org.apache.spark.sql._

import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag


object PatternQueryResolver
{
  def ResolveQuery[VD, ED:ClassTag](session: SparkSession, graph: Graph[VD, ED], query: Graph[VD => Boolean, Array[PathRegexTerm[ED]]]) : Graph[VertexId, Array[VertexPair]] =
  {
    //Initialize match sets
    val test2 = graph.vertices.collect()
    val test = graph.vertices.collect().filter(data => data._2 == "C").map(v => v._1).toSet

    val broadcastGraph = session.sparkContext.broadcast(graph)

    val queryGraph = query
      .mapVertices((id, predicate) => broadcastGraph.value.vertices.filter(data => predicate(data._2)).map(v => v._1).collect().toSet)
      .mapTriplets(t => {val test = t.attr; reachabilityQueryWithMatchSets(session, broadcastGraph, test, t.srcAttr, t.dstAttr)})

    //Just let pregel go and see what happens
    //Pregel.apply(statusGraph, initialMessage)(vertexProgram, sendMessage, mergeMessage)
    val results = Pregel.apply(queryGraph, Set[VertexId](), activeDirection = EdgeDirection.In)(vertexFunction, sendMessageStored, mergeMessage)
    results
      .mapTriplets(e => e.attr.filter(p => e.srcAttr.contains(p.source) && e.dstAttr.contains(p.dest)))
      .mapVertices((id, data) => id)
  }

  def vertexFunction(id: VertexId, vertexState: Set[VertexId], searchMessage: Set[VertexId]): Set[VertexId] =
  {
    println(s"Running at $id")
    vertexState.diff(searchMessage)
  }

  def mergeMessage(left: Set[VertexId], right: Set[VertexId]) : Set[VertexId] =
  {
    left.union(right)
  }

  def sendMessageStored(edgeTriple: EdgeTriplet[Set[VertexId], Array[VertexPair]]) : Iterator[(VertexId, Set[VertexId])] =
  {
    //Take every vertex in source attr that doesn't have a match in dest
    val select = edgeTriple.attr.filter(p => edgeTriple.dstAttr.contains(p.dest))
      .map(p => p.source).toSet
    val rmv = edgeTriple.srcAttr.filter(v => !select.contains(v))

    if (rmv.nonEmpty)
    {
      Iterator((edgeTriple.srcId, rmv))
    }
    else
    {
      Iterator[(VertexId, Set[VertexId])]()
    }
  }

  def sendMessageFactory[VD, ED:ClassTag](graph: Broadcast[Graph[VD, ED]], session: SparkSession):
    EdgeTriplet[Set[VertexId], Array[PathRegexTerm[ED]]] => Iterator[(VertexId, Set[VertexId])] =
  {
    def sendMessage(edgeTriple: EdgeTriplet[Set[VertexId], Array[PathRegexTerm[ED]]]) : Iterator[(VertexId, Set[VertexId])] =
    {
      //Ridiculous nested Pregel
      val results = reachabilityQueryWithMatchSets(session, graph, edgeTriple.attr, edgeTriple.srcAttr, edgeTriple.dstAttr)
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

  def reachabilityQueryWithMatchSets[VD, ED:ClassTag](session: SparkSession, graph: Broadcast[Graph[VD, ED]], query: Array[PathRegexTerm[ED]], matSource : Set[VertexId], matDest: Set[VertexId]) : Array[VertexPair] =
  {
    val newQuery = ReachabilityQuery[VD, ED](u1 => matSource(u1._1), u2 => matDest.contains(u2._1), query)
    ReachabilityParallelResolver.ResolveQuery(session, graph.value, newQuery, inMemoryJoin = true)
  }

}
