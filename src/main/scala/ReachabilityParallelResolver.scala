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
  hasChanged: Boolean
)

case class SearchMessage[ED]
(
  pathRegex: Array[PathRegexTerm[ED]],
  currentForwardIndex: Int,
  currentBackwardIndex: Int,
  newFrontSources: mutable.Map[VertexId, Int],
  newBackSources: mutable.Map[VertexId, Int],
  shouldCleanup: Boolean,
  isInitialMessage: Boolean,
  isForward: Boolean,
  isBackward: Boolean
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

  def vertexProgram[VD, ED](id: VertexId, vertexState: VertexState[VD], searchMessage: SearchMessage[ED]): VertexState[VD] =
  {
    if(searchMessage.shouldCleanup)
    {
      return vertexStateWithFlag(vertexState, flagVal = false)
    }
    if(searchMessage.isInitialMessage)
    {
      return vertexStateWithFlag(vertexState, flagVal = true)
    }

    //Don't need to do much.
    //Update local state with new data
    var hasChanged = false
    var forwardVal = vertexState.frontTermNumber
    var backwardVal = vertexState.backTermNumber

    var sourceMap = vertexState.sourceReachability
    var destMap = vertexState.destReachability
    if(searchMessage.isForward)
    {
      if(searchMessage.currentForwardIndex != forwardVal)
      {
        hasChanged = true
        forwardVal = searchMessage.currentForwardIndex
        //Clone to be safe
        sourceMap = searchMessage.newFrontSources.clone()
      }
      else
      {
        val didSourcesChange = addAllOrigins(vertexState.sourceReachability, searchMessage.newFrontSources)
        hasChanged = hasChanged || didSourcesChange
      }
    }
    if(searchMessage.isBackward)
    {
      if(searchMessage.currentBackwardIndex != backwardVal)
      {
        hasChanged = true
        backwardVal = searchMessage.currentBackwardIndex
        //Clone to be safe
        destMap = searchMessage.newBackSources.clone()
      }
      else
      {
        val didDestsChange = addAllOrigins(vertexState.destReachability, searchMessage.newBackSources)
        hasChanged = hasChanged || didDestsChange
      }
    }

    VertexState[VD](vertexState.data, forwardVal, sourceMap, backwardVal, destMap, hasChanged)
  }

  def vertexStateWithFlag[VD](oldState: VertexState[VD], flagVal: Boolean): VertexState[VD] =
  {
    VertexState[VD](oldState.data, oldState.frontTermNumber, oldState.sourceReachability, oldState.backTermNumber, oldState.destReachability, flagVal)
  }

  def addAllOrigins(original: mutable.Map[VertexId, Int], other: mutable.Map[VertexId, Int]) : Boolean =
  {
    var didChange = false
    for(id <- other.keySet)
    {
      if(original.contains(id))
      {
        if(original(id) > other(id))
        {
          didChange = true
          original(id) = other(id)
        }
      }
      else
      {
        didChange = true
        original(id) = other(id)
      }
    }
    didChange
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
