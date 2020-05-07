import scala.util.Random

object QueryGenerator
{
  def ReachabilityQueryFor[T](c: Int, b: Int, domain: List[T], rand: Random): List[(T, Int)] =
  {
    val k = rand.nextInt(c) + 1
    var results = List[(T, Int)]()

    var i = 0
    while(i < k)
    {
      val bound = rand.nextInt(b) + 1
      val term = domain(rand.nextInt(domain.length))

      results = results :+ (term, bound)

      i += 1
    }

    results
  }
}
