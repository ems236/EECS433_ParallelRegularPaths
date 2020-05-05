import org.apache.spark.graphx.{EdgeTriplet, VertexId, _}
import org.apache.spark.sql._

import scala.collection.mutable

//An easier model with less bookkeeping
  //Need to know if is current frontier
    //If yes dictionary of origin that can reach it -> the minRepetitionForCurrentRegexTerm
case class VertexState[VD, ED]
(
  data : VD,
  frontTermNumber: Int,
  sourceReachability: mutable.Map[VertexId, Int],
  backTermNumber: Int,
  destReachability: mutable.Map[VertexId, Int],
  hasChanged: Boolean,
  globalFrontTerm: Int,
  globalBackTerm: Int,
  globalPathRegex: Array[PathRegexTerm[ED]]
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
  def ResolveQuery[VD, ED](session: SparkSession, graph: Graph[VD, ED], reachabilityQuery: ReachabilityQuery[VD, ED]): Array[VertexPair] = {
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

  def vertexProgram[VD, ED](id: VertexId, vertexState: VertexState[VD, ED], searchMessage: SearchMessage[ED]): VertexState[VD, ED] = {
    if (searchMessage.shouldCleanup) {
      return vertexStateWithFlag(vertexState, flagVal = false)
    }
    if (searchMessage.isInitialMessage) {
      return vertexStateWithFlag(vertexState, flagVal = true)
    }

    val globalFrontTerm = searchMessage.currentForwardIndex
    val globalBackTerm = searchMessage.currentBackwardIndex

    //Don't need to do much.
    //Update local state with new data
    var hasChanged = false
    var forwardVal = vertexState.frontTermNumber
    var backwardVal = vertexState.backTermNumber

    var sourceMap = vertexState.sourceReachability
    var destMap = vertexState.destReachability
    if (searchMessage.isForward) {
      if (searchMessage.currentForwardIndex != forwardVal) {
        hasChanged = true
        forwardVal = searchMessage.currentForwardIndex
        //Clone to be safe
        sourceMap = searchMessage.newFrontSources.clone().transform((_,_) => 0)
      }
      else
      {
        val regexTerm = searchMessage.pathRegex(forwardVal - 1)
        val updated = incrementMapBelowLimit(searchMessage.newFrontSources, regexTerm)
        val didSourcesChange = addAllOrigins(vertexState.sourceReachability, updated)
        hasChanged = hasChanged || didSourcesChange
      }
    }

    if (searchMessage.isBackward) {
      if (searchMessage.currentBackwardIndex != backwardVal) {
        hasChanged = true
        backwardVal = searchMessage.currentBackwardIndex
        //Clone to be safe
        destMap = searchMessage.newBackSources.clone().transform((_,_) => 0)
      }
      else
      {
        val regexTerm = searchMessage.pathRegex(backwardVal)
        val updated = incrementMapBelowLimit(searchMessage.newBackSources, regexTerm)
        val didDestsChange = addAllOrigins(vertexState.destReachability, updated)
        hasChanged = hasChanged || didDestsChange
      }
    }

    VertexState[VD, ED](
      vertexState.data,
      forwardVal,
      sourceMap,
      backwardVal,
      destMap,
      hasChanged,
      globalFrontTerm,
      globalBackTerm,
      searchMessage.pathRegex
    )
  }

  def incrementMapBelowLimit[ED](map: mutable.Map[VertexId, Int], regexTerm: PathRegexTerm[ED]): mutable.Map[VertexId, Int] =
  {
    map.transform((_,increment) => increment + 1)
      .retain((_,increment) => !regexTerm.hasLimit() || increment <= regexTerm.limitVal())
  }

  def vertexStateWithFlag[VD, ED](oldState: VertexState[VD, ED], flagVal: Boolean): VertexState[VD, ED] = {
    VertexState[VD, ED](
      oldState.data,
      oldState.frontTermNumber,
      oldState.sourceReachability,
      oldState.backTermNumber,
      oldState.destReachability,
      flagVal,
      oldState.globalFrontTerm,
      oldState.globalBackTerm,
      oldState.globalPathRegex)
  }

  def sendMessage[VD, ED](edgeTriple: EdgeTriplet[VertexState[VD, ED], ED]): Iterator[(VertexId, SearchMessage[ED])] =
  {
    val globalFrontIndex = edgeTriple.srcAttr.globalFrontTerm
    val globalBackIndex = edgeTriple.srcAttr.globalBackTerm
    val pathRegex = edgeTriple.srcAttr.globalPathRegex

    var message = Iterator.empty
    if(edgeTriple.srcAttr.hasChanged)
    {
      if(isValidSourceEdge(edgeTriple, pathRegex, globalFrontIndex))
      {
        val toDestMsg = forwardMessage(edgeTriple.srcAttr.sourceReachability.clone(), globalFrontIndex, globalBackIndex, pathRegex)
        message ++= Iterator((edgeTriple.dstId, toDestMsg))
      }
      message ++= Iterator((edgeTriple.srcId, shutdownMessage(globalFrontIndex, globalBackIndex, pathRegex)))
    }

    if(edgeTriple.dstAttr.hasChanged)
    {
      if(isValidDestEdge(edgeTriple, pathRegex, globalFrontIndex))
      {
        val toSrcMsg = backwardMessage(edgeTriple.dstAttr.destReachability.clone(), globalFrontIndex, globalBackIndex, pathRegex)
        message ++= Iterator((edgeTriple.srcId, toSrcMsg))
      }
      message ++= Iterator((edgeTriple.dstId, shutdownMessage(globalFrontIndex, globalBackIndex, pathRegex)))
    }

    message
  }

  def isValidSourceEdge[VD, ED](edgeTriple: EdgeTriplet[VertexState[VD, ED], ED], pathRegex: Array[PathRegexTerm[ED]], globalFrontIndex: Int) : Boolean =
  {
    //Have to assume of an index before says it's changed it's the initial message and that's reasonable
    val isValidSrc = edgeTriple.srcAttr.frontTermNumber == globalFrontIndex - 1 || edgeTriple.srcAttr.frontTermNumber == globalFrontIndex
    val edgeMatches = pathRegex(globalFrontIndex - 1).doesMatch(edgeTriple.attr)
    isValidSrc && edgeMatches
  }

  def isValidDestEdge[VD, ED](edgeTriple: EdgeTriplet[VertexState[VD, ED], ED], pathRegex: Array[PathRegexTerm[ED]], globalBackIndex: Int) : Boolean =
  {
    //Have to assume of an index before says it's changed it's the initial message and that's reasonable
    val isValidDest = edgeTriple.dstAttr.backTermNumber == globalBackIndex + 1 || edgeTriple.dstAttr.backTermNumber == globalBackIndex
    val edgeMatches = pathRegex(globalBackIndex).doesMatch(edgeTriple.attr)
    isValidDest && edgeMatches
  }

  def shutdownMessage[ED](globalFrontTerm: Int, globalBackTerm: Int, globalPathRegex: Array[PathRegexTerm[ED]]) : SearchMessage[ED] =
  {
      SearchMessage[ED](
        globalPathRegex,
        globalFrontTerm,
        globalBackTerm,
        mutable.Map[VertexId, Int](),
        mutable.Map[VertexId, Int](),
        shouldCleanup = true,
        isInitialMessage = false,
        isForward = false,
        isBackward = false
      )
  }

  def forwardMessage[ED](originData: mutable.Map[VertexId, Int], globalFrontTerm: Int, globalBackTerm: Int, globalPathRegex: Array[PathRegexTerm[ED]]) : SearchMessage[ED] =
  {
    SearchMessage[ED](
      globalPathRegex,
      globalFrontTerm,
      globalBackTerm,
      originData,
      mutable.Map[VertexId, Int](),
      shouldCleanup = false,
      isInitialMessage = false,
      isForward = true,
      isBackward = false
    )
  }

  def backwardMessage[ED](originData: mutable.Map[VertexId, Int], globalFrontTerm: Int, globalBackTerm: Int, globalPathRegex: Array[PathRegexTerm[ED]]) : SearchMessage[ED] =
  {
    SearchMessage[ED](
      globalPathRegex,
      globalFrontTerm,
      globalBackTerm,
      mutable.Map[VertexId, Int](),
      originData,
      shouldCleanup = false,
      isInitialMessage = false,
      isForward = false,
      isBackward = true
    )
  }

  def mergeMessage[VD, ED](left: SearchMessage[ED], right: SearchMessage[ED]) : SearchMessage[ED] =
  {
    addAllOrigins(left.newBackSources, right.newBackSources)
    addAllOrigins(left.newFrontSources, right.newFrontSources)

    SearchMessage[ED](
      left.pathRegex,
      left.currentForwardIndex,
      left.currentBackwardIndex,
      left.newFrontSources,
      left.newBackSources,
      shouldCleanup = left.shouldCleanup && right.shouldCleanup,
      isInitialMessage = false,
      isForward = left.isForward || right.isForward,
      isBackward = left.isBackward || right.isBackward
    )
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

  def initializeVertex[VD, ED](vertexId: VertexId, vertexData: VD, query: ReachabilityQuery[VD, ED]) : VertexState[VD, ED] =
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

    VertexState(vertexData, sourceReach, sourceMap, destReach, destMap, false, 0, query.pathExpression.length, query.pathExpression)
  }


}
