class PathRegexTerm[ED](var edgePredicate: (ED => Boolean), var limit: Option[Int])
{
  def hasLimit() = limit match {
    case Some(x) => true
    case None => false
  }

  def limitVal() = limit match {
    case Some(x) => x
    case None => 0
  }

  def doesMatch(edge: ED) = edgePredicate
}
