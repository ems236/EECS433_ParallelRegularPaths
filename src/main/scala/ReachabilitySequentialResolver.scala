import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.sql._

import scala.collection.mutable


case class VertexPair(source: VertexId, dest: VertexId)
case class ReachabilityQuery[VD, ED](sourceFilter: ((VertexId, VD)) => Boolean,
                                     destFilter: ((VertexId, VD)) => Boolean,
                                     pathExpression: Array[PathRegexTerm[ED]])
case class TermStatus(isFrontier: Boolean, middlestIndex: Int, originIds: mutable.Set[VertexId])

object ReachabilitySequentialResolver
{
  def ResolveQuery[VD, ED](session: SparkSession, graph: Graph[VD, ED], reachabilityQuery: ReachabilityQuery[VD, ED]) : Array[VertexPair] =
  {
    //Bi-directional search
    val sources = graph.vertices.filter(reachabilityQuery.sourceFilter)
    val dests = graph.vertices.filter(reachabilityQuery.destFilter)

    val lastIndex = reachabilityQuery.pathExpression.length - 1
    var source_set = sources.map(v => (v._1, TermStatus(true, 0, mutable.Set(v._1))))
      .collectAsMap()
    var mutable_source_set = collection.mutable.Map(source_set.toSeq: _*)

    var dest_set = dests.map(v => (v._1, TermStatus(true, lastIndex + 1, mutable.Set(v._1))))
      .collectAsMap()
    var mutable_dest_set = collection.mutable.Map(dest_set.toSeq: _*)

    //Start 1 off because not moving through the graph yet
    //is the current value that has been processed
    var currentForward = 0
    var currentBackward = lastIndex + 1

    while(currentForward < currentBackward)
    {
      if(source_set.size > dest_set.size)
      {
        currentBackward -= 1
        expandFrontier(graph, mutable_dest_set, reachabilityQuery.pathExpression(currentBackward), currentBackward, currentBackward + 1, isForward = false)
      }
      else
      {
        expandFrontier(graph, mutable_source_set, reachabilityQuery.pathExpression(currentForward), currentForward + 1, currentForward, isForward = true)
        currentForward += 1
      }
    }

    println(s"Parsing results, ends met at $currentForward")
    resultIntersection(session, mutable_source_set, mutable_dest_set, currentForward)

  }

  def expandFrontier[VD, ED](graph: Graph[VD, ED]
     , vertexMap: mutable.Map[VertexId, TermStatus]
     , regexTerm: PathRegexTerm[ED]
     , reachedIndex: Int
     , previousIndex: Int
     , isForward: Boolean): Unit =
  {
    println(s"Expanding frontier for regex term ${if (isForward) reachedIndex - 1 else reachedIndex}")
    //take all the frontiers
    var currentFrontier = vertexMap.filter(v => v._2.isFrontier).map(v => (v._1, v._2.originIds.toSet))
    val termFrontier: mutable.Map[VertexId, TermStatus] = mutable.Map()
    val hasLimit = regexTerm.hasLimit()
    val expandCount = regexTerm.limitVal()

    var reachedCount = 0

    //do their expansion in a while loop until its done
    var currentCount = 0
    var nextFrontier: mutable.Map[VertexId, Set[VertexId]] = mutable.Map()
    while (currentFrontier.nonEmpty && (!hasLimit || currentCount < expandCount))
    {
      println(s"Frontier has ${currentFrontier.size} elements. Current is $currentCount. Limit is $expandCount")
      for(vertex <- currentFrontier)
      {
        //println(s"Exploring from vertex ${vertex._1}")

        val edgeTargetId = (e: Edge[ED]) => {if (isForward) e.srcId else e.dstId}
        val edgeExploreId = (e: Edge[ED]) => {if (isForward) e.dstId else e.srcId}
        val newVertices = graph.edges
          .filter(e => edgeTargetId(e) == vertex._1 && regexTerm.doesMatch(e.attr))
          .map(e => edgeExploreId(e))
          .distinct()
          .collect()

        for(newVertex <- newVertices)
        {
          println(s"New Vertex reached ${vertex._1} to ${newVertex}")
          reachedCount += 1
          val shouldAdd = reachNewVertex(newVertex, termFrontier, vertex._2, reachedIndex)
          //add to new frontier
          if (shouldAdd)
          {
              println("Adding to new frontier (should not get here)")
              nextFrontier(newVertex) = termFrontier(newVertex).originIds.toSet
          }
        }
      }

      println(s"Have reached $reachedCount")
      //collect new frontier into frontier
      currentFrontier = nextFrontier
      nextFrontier = mutable.Map()

      currentCount += 1
    }


    //Clear all reachability info for non-frontier
    clearOldFrontier(vertexMap, previousIndex)
    addNewFrontier(vertexMap, termFrontier)
  }

  def reachNewVertex(newVertex: VertexId, termFrontier: mutable.Map[VertexId, TermStatus], originSet:  Set[VertexId], regexIndex: Int): Boolean =
  {
    var shouldAdd = true
    //if exists in vertex set, need to update it
    if(termFrontier.contains(newVertex))
    {
      //Dont visit the same vertex twice from the same places

      println(s"newOriginSet $originSet")
      val oldTermSet = termFrontier(newVertex).originIds
      println(s"old set $oldTermSet")
      val oldLength = oldTermSet.size
      oldTermSet ++= originSet

      if(oldTermSet.size == oldLength)
      {
        println("Nothing new (Shouldn't get here)")
        //Dont visit the same vertex twice from the same places
        //If nothing changes, don't add this to the new frontier
        shouldAdd = false
      }
      //copy same set over
      val newTermStatus = TermStatus(isFrontier = true, regexIndex, oldTermSet)
      termFrontier(newVertex) = newTermStatus
    }
    else
    {
      println("New vertex (Shouldn't get here)")

      //else add new vertex to vertex set
      val newSet = mutable.Set[VertexId]()
      newSet ++= originSet
      val newTermStatus = TermStatus(isFrontier = true, regexIndex, newSet)
      termFrontier(newVertex) = newTermStatus
    }

    shouldAdd
  }

  def clearOldFrontier(vertexMap: mutable.Map[VertexId, TermStatus], clearIndex: Int): Unit =
  {
    val oldFrontiers = vertexMap.filter(v => v._2.middlestIndex == clearIndex).keySet
    println(s"clearing frontier at $clearIndex. Has ${oldFrontiers.size} nodes")

    for(id <- oldFrontiers)
    {
      vertexMap(id) = TermStatus(isFrontier = false, clearIndex, mutable.Set())
    }
  }

  def addNewFrontier(vertexMap: mutable.Map[VertexId, TermStatus], newFrontier: mutable.Map[VertexId, TermStatus]): Unit =
  {
    for(vertex <- newFrontier.keySet)
    {
      vertexMap(vertex) = newFrontier(vertex)
    }
  }

  def intersectionPointsToDF(session: SparkSession, vertexSet: mutable.Map[VertexId, TermStatus], meetIndex: Int, description: String): DataFrame =
  {
    import session.implicits._

    //filter both by meeting index and put them in a dataset
    //flatten so every vertex paired with its origin
    vertexSet
      .filter(v => v._2.middlestIndex == meetIndex && v._2.originIds.size > 0)
      .flatMap(v => v._2.originIds.map(origin => (v._1, origin)))
      .toSeq
      .toDF("Id", description)
  }

  def resultIntersection(session: SparkSession, sourceSet: mutable.Map[VertexId, TermStatus], destSet: mutable.Map[VertexId, TermStatus], meetIndex: Int): Array[VertexPair] =
  {
    import session.implicits._
    val SOURCE = "source"
    val DEST = "dest"

    val sourceData = intersectionPointsToDF(session, sourceSet, meetIndex, SOURCE)
    val destData = intersectionPointsToDF(session, destSet, meetIndex, DEST)

    println(s"Joining source ${sourceData.count()} to dest ${destData.count()}")

    //join both
    //flatten essentially makes this cartesian product of two origin sets
    //filter duplicates
    sourceData
      .join(destData, "Id")
      .select(SOURCE, DEST)
      .distinct()
      .map(row => VertexPair(row.getLong(0), row.getLong(1)))
      .collect()
  }
}
