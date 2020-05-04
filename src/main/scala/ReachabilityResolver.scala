import org.apache.spark.graphx._
import scala.collection.mutable.Map

case class VertexPair(source: VertexId, dest: VertexId)
case class ReachabilityQuery[VD, ED](sourceFilter: ((VertexId, VD)) => Boolean,
                                     destFilter: ((VertexId, VD)) => Boolean,
                                     pathExpression: Array[PathRegexTerm[ED]])

case class TermStatus(isFrontier: Boolean, middlestIndex: Int, originIds: Array[VertexId])

object ReachabilityResolver
{
  def ResolveQuery[VD, ED](graph: Graph[VD, ED], reachabilityQuery: ReachabilityQuery[VD, ED]) : List[VertexPair] =
  {
    //Bi-directional search
    val sources = graph.vertices.filter(reachabilityQuery.sourceFilter)
    val dests = graph.vertices.filter(reachabilityQuery.destFilter)

    val lastIndex = reachabilityQuery.pathExpression.length - 1
    var source_set = VertexRDD[TermStatus](sources.map(v => (v._1, TermStatus(true, -1, Array(v._1)))))
    var dest_set = VertexRDD[TermStatus](dests.map(v => (v._1, TermStatus(true, lastIndex + 1, Array(v._1)))))

    //Start 1 off because not moving through the graph yet
    //is the current value that has been processed
    var currentForward = -1
    var currentBackward = lastIndex + 1

    while(currentForward < currentBackward)
    {
      if(source_set.count() > dest_set.count())
      {
        currentBackward -= 1
        dest_set = expandedFrontier(graph, dest_set, reachabilityQuery.pathExpression, currentBackward)
      }
      else
      {
        currentForward += 1
        source_set = expandedFrontier(graph, dest_set, reachabilityQuery.pathExpression, currentForward)
      }
    }

    return intersectResults(source_set, dest_set, currentBackward)

  }

  def expandedFrontier[VD, ED](graph: Graph[VD, ED]
     , vertexSet: VertexRDD[TermStatus]
     , regex: Array[PathRegexTerm[ED]]
     , regexIndex: Int) : VertexRDD[TermStatus] =
  {
    //take all the frontiers
    //Honestly not worth doing this in an RDD
    val frontier = vertexSet.filter(v => v._2.isFrontier).collect()
    val newFrontier: Map[VertexId, TermStatus] = Map()
    val regexTerm = regex(regexIndex)
    val expandCount = if (regexTerm.hasLimit()) 1 else regexTerm.limitVal()

    //do their expansion in a while loop until its done
    var currentCount = 0
    while (currentCount < expandCount)
    {
      
    }


      //move all the source nodes forward as you do this
  }

  def intersectResults(sourceSet: VertexRDD[TermStatus], destSet: VertexRDD[TermStatus], meetIndex: Int): Array[VertexPair] =
  {
    //filter both by meeting index
    //join both
    //cartesian product of two origin sets
    //filter duplicates
  }
}
