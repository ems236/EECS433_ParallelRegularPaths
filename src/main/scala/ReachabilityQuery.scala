import org.apache.spark.graphx.VertexId

case class ReachabilityQuery[VD, ED](sourceFilter: ((VertexId, VD)) => Boolean,
                                     destFilter: ((VertexId, VD)) => Boolean,
                                     pathExpression: Array[PathRegexTerm[ED]])
