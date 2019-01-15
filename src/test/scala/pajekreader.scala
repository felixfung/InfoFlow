/***************************************************************************
 * Test Suite for reading Pajek files
 ***************************************************************************/

class PajekReaderTest extends SparkTestSuite
{
  val logFile = new LogFile(sc,"","","","","","",false)

  test("Throw error when reading wrong file") {
    val thrown = intercept[Exception] {
      val dummy = PajekReader(sc,"Nets/dummy",logFile)
    }
    assert( thrown.getMessage === "Cannot open file Nets/dummy" )
  }

  test("Read trivial network with comment") {
    val graph = PajekReader(sc,"Nets/zero.net",logFile)
    assert( graph.vertices.collect === Array((1,("v1",1))) )
    assert( graph.edges.collect === Array() )
  }

  test("Read trivial networks") {
    val graph = PajekReader(sc,"Nets/trivial.net",logFile)
    assert( graph.vertices.collect.sorted ===
      Array((1,("m01",1)),(2,("m02",2))) )
    assert( graph.edges.collect === Array( (1,(2,2)) ) )
  }

  test("Read trivial networks with self loop") {
    val graph = PajekReader(sc,"Nets/trivial-with-self-loop.net",logFile)
    assert( graph.vertices.collect.sorted ===
      Array((1,("m01",1)),(2,("m02",2))) )
    assert( graph.edges.collect.sorted === Array( (1,(2,2)), (2,(2,1)) ) )
  }

  test("Read simple network") {
    val graph = PajekReader(sc,"Nets/simple.net",logFile)
    assert( graph.vertices.collect.sorted ===
      Array(
        (1,("1",1)),
        (2,("2",2)),
        (3,("3",3)),
        (4,("4",4)),
        (5,("5",5)),
        (6,("6",6))
      )
    )
    assert( graph.edges.collect.sorted ===
      Array(
        (1,(2,1.0)),
        (1,(3,1.0)),
        (2,(1,1.0)),
        (2,(3,1.0)),
        (3,(1,1.0)),
        (3,(2,1.0)),
        (3,(4,0.5)),
        (4,(3,0.5)),
        (4,(5,1.0)),
        (4,(6,1.0)),
        (5,(4,1.0)),
        (5,(6,1.0)),
        (6,(4,1.0)),
        (6,(5,1.0))
      )
    )
  }

  test("Read file with *edgeslist format") {
    val graph = PajekReader(sc,"Nets/edge-test.net",logFile)
    assert( graph.vertices.collect.sorted === Array(
      (1,("1",1)),
      (2,("2",2)),
      (3,("3",3)),
      (4,("4",4)),
      (5,("5",5)),
      (6,("6",6))
    ) )
    assert( graph.edges.collect.sorted === Array(
      (1,(2,1)), (1,(3,1)), (1,(4,1)),
      (2,(1,1)), (2,(2,1)), (2,(6,1))
    ) )
  }

  test("Test reading arcs list") {
    val graph = PajekReader(sc,"Nets/arcslist-test.net",logFile)
    assert( graph.vertices.collect === Array(
      (1,("1",1)),
      (2,("2",2)),
      (3,("3",3)),
      (4,("4",4)),
      (5,("5",5)),
      (6,("6",6))
    ) )
    assert( graph.edges.collect.sorted === Array(
      (1,(2,1)), (1,(3,1)), (1,(4,1)),
      (2,(1,1)), (2,(2,1)), (2,(6,1)),
      (3,(2,1)), (3,(4,1))
    ) )
  }
}
