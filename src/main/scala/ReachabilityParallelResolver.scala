import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.sql._

import scala.collection.mutable

//An easier model with less bookkeeping
  //Need to know if is current frontier
    //If yes dictionary of origin that can reach it -> the minRepetitionForCurrentRegexTerm
case class VertexState[VD]
(
  data : VD,
  frontTermNumber: Int,
  sourceReachability: mutable.Map[VertexId, Int],
  backTermNumber: Int,
  destReachability: mutable.Map[VertexId, Int],
)

case class SearchMessage[ED]
(
  pathRegex: Array[PathRegexTerm[ED]],
  currentForwardIndex: Int,
  currentBackwardIndex: Int,
  newFrontSources: mutable.Map[VertexId, Int],
  newBackSources: mutable.Map[VertexId, Int],
)

object ReachabilityParallelResolver {
  def ResolveQuery[VD, ED](session: SparkSession, graph: Graph[VD, ED], reachabilityQuery: ReachabilityQuery[VD, ED]) : Array[VertexPair] =
  {
    //Could do Pregel to id source and dest set
    //Need a convenient vertex structure anyway so map vertices is more appropriate
    //I'm sure it gets parallelized anyway
    var statusGraph = graph.mapVertices((v, data) => initializeVertex(v, data, reachabilityQuery))

    //bidirectional search
    // while start != end
      //Pregel to expand both frontiers by 1
      //Have some care to not overshoot

    //Extract results where they meet in the middle
    //Pregel.apply()
  }

  def vertexProgram[VD, ED](id: VertexId, vertexState: VertexState[VD], searchMessage: SearchMessage[ED]): Unit =
  {
    //Don't need to do much.
    //Update local state with new
  }

  def initializeVertex[VD, ED](vertexId: VertexId, vertexData: VD, query: ReachabilityQuery[VD, ED]) : VertexState[VD] =
  {
    var sourceReach = -1
    var destReach = -1
    val sourceMap = mutable.Map[VertexId, Int]()
    val destMap = mutable.Map[VertexId, Int]()
    if(query.sourceFilter((vertexId, vertexData)))
    {
      sourceReach = 0
      sourceMap(vertexId) = 0
    }
    if(query.sourceFilter((vertexId, vertexData)))
    {
      destReach = query.pathExpression.length
      destMap(vertexId) = 0
    }

    VertexState(vertexData, sourceReach, sourceMap, destReach, destMap)
  }


}
