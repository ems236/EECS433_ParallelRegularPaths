import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.sql._

import scala.collection.mutable


case class VertexPair(source: VertexId, dest: VertexId)
case class ReachabilityQuery[VD, ED](sourceFilter: ((VertexId, VD)) => Boolean,
                                     destFilter: ((VertexId, VD)) => Boolean,
                                     pathExpression: Array[PathRegexTerm[ED]])
//case class TermStatus(isFrontier: Boolean, middlestIndex: Int, originIds: mutable.Set[VertexId])

object ReachabilitySequentialResolver
{
  type OriginSet = mutable.Set[VertexId]

  def verticesToFrontier[VD](vertices: VertexRDD[VD]) : mutable.Map[VertexId, OriginSet] =
  {
    val vertexSet = vertices.map(v => (v._1, mutable.Set(v._1)))
      .collectAsMap()
    collection.mutable.Map(vertexSet.toSeq: _*)
  }

  def ResolveQuery[VD, ED](session: SparkSession, graph: Graph[VD, ED], reachabilityQuery: ReachabilityQuery[VD, ED]) : Array[VertexPair] =
  {
    //Bi-directional search
    val sources = graph.vertices.filter(reachabilityQuery.sourceFilter)
    val dests = graph.vertices.filter(reachabilityQuery.destFilter)

    val lastIndex = reachabilityQuery.pathExpression.length - 1

    var sourceFrontier = verticesToFrontier(sources)
    var destFrontier = verticesToFrontier(dests)

    //Start 1 off because not moving through the graph yet
    //is the current value that has been processed
    var currentForward = 0
    var currentBackward = lastIndex + 1

    while(currentForward < currentBackward)
    {
      if(sourceFrontier.size > destFrontier.size)
      {
        currentBackward -= 1
        destFrontier = expandFrontier(graph, destFrontier, reachabilityQuery.pathExpression(currentBackward), currentBackward, isForward = false)
      }
      else
      {
        sourceFrontier = expandFrontier(graph, sourceFrontier, reachabilityQuery.pathExpression(currentForward), currentForward + 1, isForward = true)
        currentForward += 1
      }
    }

    println(s"Parsing results, ends met at $currentForward")
    resultIntersection(session, sourceFrontier, destFrontier, currentForward)

  }

  def expandFrontier[VD, ED](
    graph: Graph[VD, ED]
    , currentFrontier: mutable.Map[VertexId, OriginSet]
    , regexTerm: PathRegexTerm[ED]
    , reachedIndex: Int
    , isForward: Boolean): mutable.Map[VertexId, OriginSet] =
  {
    println(s"Expanding frontier for regex term ${if (isForward) reachedIndex - 1 else reachedIndex}")
    //take all the frontiers
    var currentIterationFrontier = currentFrontier.map(v => (v._1, v._2.toSet))
    val newFrontier: mutable.Map[VertexId, OriginSet] = mutable.Map()
    val hasLimit = regexTerm.hasLimit()
    val expandCount = regexTerm.limitVal()

    var reachedCount = 0

    //do their expansion in a while loop until its done
    var currentCount = 0
    var nextIterationFrontier: mutable.Map[VertexId, Set[VertexId]] = mutable.Map()
    while (currentIterationFrontier.nonEmpty && (!hasLimit || currentCount < expandCount))
    {
      println(s"Frontier has ${currentIterationFrontier.size} elements. Current is $currentCount. Limit is $expandCount")
      for(vertex <- currentIterationFrontier)
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
          val shouldAdd = reachNewVertex(newVertex, newFrontier, vertex._2)
          //add to new frontier
          if (shouldAdd)
          {
              println("Adding to new frontier")
              nextIterationFrontier(newVertex) = newFrontier(newVertex).toSet
          }
        }
      }

      println(s"Have reached $reachedCount")
      //collect new frontier into frontier
      currentIterationFrontier = nextIterationFrontier
      nextIterationFrontier = mutable.Map()

      currentCount += 1
    }


    //Clear all reachability info for non-frontier
    //clearOldFrontier(currentFrontier, previousIndex)
    //addNewFrontier(currentFrontier, termFrontier)
    return newFrontier
  }

  def reachNewVertex(newVertex: VertexId, termFrontier: mutable.Map[VertexId, OriginSet], originSet:  Set[VertexId]): Boolean =
  {
    var shouldAdd = true
    //if exists in vertex set, need to update it
    if(termFrontier.contains(newVertex))
    {
      //Dont visit the same vertex twice from the same places

      println(s"newOriginSet $originSet")
      val oldTermSet = termFrontier(newVertex)
      println(s"old set $oldTermSet")
      val oldLength = oldTermSet.size
      oldTermSet ++= originSet

      if(oldTermSet.size == oldLength)
      {
        //println("Nothing new")
        //Dont visit the same vertex twice from the same places
        //If nothing changes, don't add this to the new frontier
        shouldAdd = false
      }
      //copy same set over
      termFrontier(newVertex) = oldTermSet
    }
    else
    {
      //println("New vertex")

      //else add new vertex to vertex set
      val newSet = mutable.Set[VertexId]()
      newSet ++= originSet
      termFrontier(newVertex) = newSet
    }

    shouldAdd
  }

  /*
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
  }*/

  def intersectionPointsToDF(session: SparkSession, vertexSet: mutable.Map[VertexId, OriginSet], meetIndex: Int, description: String): DataFrame =
  {
    import session.implicits._

    //filter both by meeting index and put them in a dataset
    //flatten so every vertex paired with its origin
    vertexSet
      .flatMap(v => v._2.map(origin => (v._1, origin)))
      .toSeq
      .toDF("Id", description)
  }

  def resultIntersection(session: SparkSession, sourceSet: mutable.Map[VertexId, OriginSet], destSet: mutable.Map[VertexId, OriginSet], meetIndex: Int): Array[VertexPair] =
  {
    import session.implicits._
    val SOURCE = "source"
    val DEST = "dest"

    println(s"Joining source ${sourceSet.keySet.size} to dest ${destSet.keySet.size}")

    var i = 0
    sourceSet.foreach(v => {
      i += v._2.size
      println(s"$i ${v._2} -> ${v._1}")
    })

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
