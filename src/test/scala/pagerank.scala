/***************************************************************************
 * Test Suite for Page Rank algorithm
 ***************************************************************************/

import org.scalactic.TolerantNumerics

class PageRankTest extends SparkTestSuite
{
  // Comparisons of doubles has a tolerance of 1e-5
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(1e-2)
  val logFile = new LogFile(sc,"","","","","","",false)

  test("Read trivial network with self loop in pure random walk") {
    val graph = PajekReader(sc,"Nets/trivial-with-self-loop.net",logFile)
    assert( graph.edges.collect.sorted === Array( (1,(2,2)), (2,(2,1)) ) )
    val freq = PageRank(graph,1).collect.sorted
    assert( freq(0)._2.toDouble === 0.0 )
    assert( freq(1)._2.toDouble === 1.0 )
  }

  test("Read trivial network with self loop") {
    val graph = PajekReader(sc,"Nets/trivial-with-self-loop.net",logFile)
    val freq = PageRank(graph,.85).collect.sorted
    assert( freq(0)._2.toDouble === 0.0725 )
    assert( freq(1)._2.toDouble === 0.925 )
  }

  test("Read trivial network with dangling node in pure random walk") {
    val graph = PajekReader(sc,"Nets/trivial.net",logFile)
    val freq = PageRank(graph,1).collect.sorted
    assert( freq(0)._2.toDouble === 0.333 )
    assert( freq(1)._2.toDouble === 0.667 )
  }

  test("Read trivial network with dangling node") {
    val graph = PajekReader(sc,"Nets/trivial.net",logFile)
    val freq = PageRank(graph,.85).collect.sorted
    assert( freq(0)._2.toDouble === 0.35 )
    assert( freq(1)._2.toDouble === 0.65 )
  }

  test("Read one node network with no link") {
    val graph = PajekReader(sc,"Nets/zero.net",logFile)
    assert( PageRank(graph,.85).collect.sorted === Array((1,1.0)) )
  }

  test("Read Nets/small-asym.net") {
    val graph = PajekReader(sc,"Nets/small-asym.net",logFile)
    val freq = PageRank(graph,.85).collect.sorted.map{case(idx,x)=>x}
    assert( freq.sum === 1.0 )
  }
}
