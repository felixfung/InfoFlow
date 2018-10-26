/***************************************************************************
 * Test Suite for infoMap algorithm
 * strategy is to perform functional tests
 * that reads in local Pajek net files
 * and perform community detection
 * and check partitioning results
 * and code length
 ***************************************************************************/

class InfoMapTest extends SparkTestSuite
{

  val infoMap = new InfoMap

  test("Test trivial network") {
    val( codelength, partition ) = CommunityDetectionTest( sc, infoMap,
      "Nets/trivial.net" )
    assert( Math.abs( codelength -1.45 ) < 0.1 )
    assert( partition == Array( (1,1), (2,1) ) )
  }

  test("Test small network") {
    val( codelength, partition ) = CommunityDetectionTest( sc, infoMap,
      "Nets/small.net" )
    assert( Math.abs( codelength -1.58 ) < 0.1 )
    assert( partition == Array( (1,1), (2,1), (3,3), (4,3) ) )
  }

  test("Test small asymmetric network") {
    val( codelength, partition ) = CommunityDetectionTest( sc, infoMap,
      "Nets/small-asym.net" )
    assert( Math.abs( codelength -1.38 ) < 0.1 )
    assert( partition == Array( (1,1), (2,1), (3,3) ) )
  }

  test("Read simple test network") {
    val( codelength, partition ) = CommunityDetectionTest( sc, infoMap,
      "Nets/simple.net" )
    assert( Math.abs( codelength -2.38 ) < 0.1 )
    assert( partition == Array(  (1,1), (2,1), (3,1), (4,4), (5,4), (6,4) ) )
  }

  test("Reproduce Rosvall and Bergstrom 2008 result") {
    val( codelength, partition ) = CommunityDetectionTest( sc, infoMap,
      "Nets/rosvall.net" )
    assert( Math.abs( codelength -3.51 ) < 0.1 )
    assert( partition == Array(
      (1,1), (2,1), (3,1), (4,1), (5,1), (6,1),
      (7,7), (8,7), (9,7), (10,7), (11,7), (12,7), (13,7),
      (14,14), (15,14), (16,14), (17,14), (18,14), (19,14), (20,14), (21,14),
      (22,22), (23,22), (24,22), (25,22)
    ))
  }

  test("infoMap vs modularity test 1") {
    val( codelength, partition ) = CommunityDetectionTest( sc, infoMap,
      "Nets/infoflow-vs-modularity1.net" )
    assert( Math.abs( codelength -3.43 ) < 0.1 )
    assert( partition == Array(
      (1,1), (2,1), (3,1), (4,1),
      (5,5), (6,5), (7,5), (8,5),
      (9,9), (10,9), (11,9), (12,9),
      (13,13), (14,13), (15,13), (16,13)
    ))
  }

  test("infoMap vs modularity test 2") {
    val( codelength, partition ) = CommunityDetectionTest( sc, infoMap,
      "Nets/infoflow-vs-modularity2.net" )
    assert( Math.abs( codelength -2.68 ) < 0.1 )
    assert( partition == Array(
      (1,1), (2,1), (3,1), (4,1),
      (5,5), (6,5), (7,5), (8,5),
      (9,9), (10,9), (11,9), (12,9),
      (13,13), (14,13), (15,13), (16,13)
    ))
  }
}
