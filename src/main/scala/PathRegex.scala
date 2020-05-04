import java.io

@SerialVersionUID(1L)
class PathRegexTerm[ED](var edgePredicate: (ED => Boolean), var limit: Option[Int]) extends Serializable
{
  def hasLimit() = limit match {
    case Some(x) => true
    case None => false
  }

  def limitVal() = limit match {
    case Some(x) => x
    case None => 0
  }

  def doesMatch = edgePredicate
}