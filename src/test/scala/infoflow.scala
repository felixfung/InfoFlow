/***************************************************************************
 * Test Suite for InfoFlow algorithm
 * strategy is to perform functional tests
 * that reads in local Pajek net files
 * and perform community detection
 * and check partitioning results
 * and code length
 ***************************************************************************/

class InfoFlowTest extends SparkTestSuite
{
  val infoFlow = new InfoFlow("asymmetric")

  test("Test trivial network") {
    val( codelength, partition, graph, _ )
      = CommunityDetectionTest( sc, infoFlow, "Nets/trivial.net" )
    assert( Math.abs( codelength -0.94 ) < 0.01 )
    assert( partition.sameElements(Array( (1,1), (2,1) ) ) )
    assert( graph.edges.collect.sorted.sameElements(Array( (1,(2,2)) )) )
  }

  test("Test small network") {
    val( codelength, partition, graph, part )
      = CommunityDetectionTest( sc, infoFlow, "Nets/small.net" )
    assert( Math.abs( codelength -1.58 ) < 0.01 )
    assert( partition.sameElements(Array( (1,1), (2,1), (3,3), (4,3) )) )
    assert( graph.edges.collect.sorted.sameElements(Array(
      (1,(2,1)), (2,(1,1)), (3,(4,1)), (4,(3,1))
    )) )
    assert( part.edges.collect.sorted.isEmpty )
  }

  test("Test small asymmetric network") {
    val( codelength, partition, _, _ )
      = CommunityDetectionTest( sc, infoFlow, "Nets/small-asym.net" )
    assert( Math.abs( codelength -1.38 ) < 0.01 )
    assert( partition.sameElements(Array( (1,1), (2,1), (3,3) )) )
  }

  test("Read simple test network") {
    val( codelength, partition, _, _ )
      = CommunityDetectionTest( sc, infoFlow, "Nets/simple.net" )
    assert( Math.abs( codelength -2.38 ) < 0.01 )
    assert( partition.sameElements(Array(
      (1,1), (2,1), (3,1), (4,4), (5,4), (6,4)
    )) )
  }

  test("Reproduce Rosvall and Bergstrom 2008 result") {
    val( codelength, partition, _, part )
      = CommunityDetectionTest( sc, infoFlow, "Nets/rosvall.net" )
    assert( Math.abs( codelength -3.51 ) < 0.01 )
    assert( partition.sameElements(Array(
      (1,1), (2,1), (3,1), (4,1), (5,1), (6,1),
      (7,7), (8,7), (9,7), (10,7), (11,7), (12,7), (13,7),
      (14,14), (15,14), (16,14), (17,14), (18,14), (19,14), (20,14), (21,14),
      (22,22), (23,22), (24,22), (25,22)
    )) )
    assert( part.edges.collect.sorted.sameElements(Array(
      (1,(7,0.0014197427450954197)),
      (7,(1,8.212044028182088E-4)),
      (7,(14,0.011129243900469512)),
      (7,(22,0.003329979932838147)),
      (14,(7,0.007803811628658931)),
      (14,(22,0.0034855554373641362)),
      (22,(7,0.003497187478727881)),
      (22,(14,0.005189045077556212))
    )) )
  }
}
