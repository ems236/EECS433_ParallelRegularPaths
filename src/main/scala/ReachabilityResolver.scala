import org.apache.spark.graphx._
import org.apache.spark.sql._
import scala.collection.mutable


case class VertexPair(source: VertexId, dest: VertexId)
case class ReachabilityQuery[VD, ED](sourceFilter: ((VertexId, VD)) => Boolean,
                                     destFilter: ((VertexId, VD)) => Boolean,
                                     pathExpression: Array[PathRegexTerm[ED]])

case class TermStatus(isFrontier: Boolean, middlestIndex: Int, originIds: mutable.Set[VertexId])

object ReachabilityResolver
{
  def ResolveQuery[VD, ED](session: SparkSession, graph: Graph[VD, ED], reachabilityQuery: ReachabilityQuery[VD, ED]) : Array[VertexPair] =
  {
    //Bi-directional search
    val sources = graph.vertices.filter(reachabilityQuery.sourceFilter)
    val dests = graph.vertices.filter(reachabilityQuery.destFilter)

    val lastIndex = reachabilityQuery.pathExpression.length - 1
    var source_set = sources.map(v => (v._1, TermStatus(true, -1, mutable.Set(v._1))))
      .collectAsMap()
    var mutable_source_set = collection.mutable.Map(source_set.toSeq: _*)

    var dest_set = dests.map(v => (v._1, TermStatus(true, lastIndex + 1, mutable.Set(v._1))))
      .collectAsMap()
    var mutable_dest_set = collection.mutable.Map(dest_set.toSeq: _*)

    //Start 1 off because not moving through the graph yet
    //is the current value that has been processed
    var currentForward = -1
    var currentBackward = lastIndex + 1

    while(currentForward < currentBackward)
    {
      if(source_set.size > dest_set.size)
      {
        currentBackward -= 1
        expandFrontier(graph, mutable_dest_set, reachabilityQuery.pathExpression, currentBackward, currentBackward + 1)
      }
      else
      {
        currentForward += 1
        expandFrontier(graph, mutable_source_set, reachabilityQuery.pathExpression, currentForward, currentForward - 1)
      }
    }

    resultIntersection(session, mutable_source_set, mutable_dest_set, currentForward)

  }

  def expandFrontier[VD, ED](graph: Graph[VD, ED]
     , vertexMap: mutable.Map[VertexId, TermStatus]
     , regex: Array[PathRegexTerm[ED]]
     , regexIndex: Int
     , previousIndex: Int): Unit =
  {
    //take all the frontiers
    var frontier = vertexMap.filter(v => v._2.isFrontier).map(v => (v._1, v._2.originIds.toSet))
    var newFrontier: mutable.Map[VertexId, Set[VertexId]] = mutable.Map()
    val regexTerm = regex(regexIndex)
    val hasLimit = regexTerm.hasLimit()
    val expandCount = regexTerm.limitVal()

    //do their expansion in a while loop until its done
    var currentCount = 0
    while (frontier.nonEmpty && (!hasLimit || currentCount < expandCount))
    {
      for(vertex <- frontier)
      {
        val newVertices = graph.edges
          .filter(e => e.srcId == vertex._1 && regexTerm.doesMatch(e.attr))
          .map(e => e.dstId)
          .distinct()
          .collect()

        for(newVertex <- newVertices) {
          reachNewVertes(newVertex, vertexMap, vertex._2, regexIndex)
          //add to new frontier
          newFrontier(newVertex) = vertexMap(newVertex).originIds.toSet
        }
      }

      //collect new frontier into frontier
      frontier = newFrontier
      newFrontier = mutable.Map()

      currentCount += 1
    }


    //Clear all reachability info for non-frontier
    clearOldFrontier(vertexMap, previousIndex)
  }

  def reachNewVertes(newVertex: VertexId, vertexMap: mutable.Map[VertexId, TermStatus], originSet:  Set[VertexId], regexIndex: Int): Unit =
  {
    //if exists in vertex set, need to update it
    if(vertexMap.contains(newVertex))
    {
      val oldTermSet = vertexMap(newVertex).originIds
      oldTermSet ++= originSet
      //copy same set over
      val newTermStatus = TermStatus(isFrontier = true, regexIndex, oldTermSet)
      vertexMap(newVertex) = newTermStatus
    }
    else
    {
      //else add new vertex to vertex set
      val newSet = mutable.Set[VertexId]()
      newSet ++= originSet
      val newTermStatus = TermStatus(isFrontier = true, regexIndex, newSet)
      vertexMap(newVertex) = newTermStatus
    }
  }

  def clearOldFrontier(vertexMap: mutable.Map[VertexId, TermStatus], clearIndex: Int): Unit =
  {
    val oldFrontiers = vertexMap.filter(v => v._2.middlestIndex == clearIndex).keySet
    for(id <- oldFrontiers)
    {
      vertexMap(id) = TermStatus(isFrontier = false, clearIndex, mutable.Set())
    }
  }

  def intersectionPointsToDF(session: SparkSession, vertexSet: mutable.Map[VertexId, TermStatus], meetIndex: Int, description: String): DataFrame =
  {
    import session.implicits._

    //filter both by meeting index and put them in a dataset
    //flatten so every vertex paired with its origin
    vertexSet
      .filter(v => v._2.middlestIndex == meetIndex)
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
