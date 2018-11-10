/***************************************************************************
 * Test Suite for testing the DFS edge labeling code
 ***************************************************************************/

import org.apache.spark.rdd.RDD

class EdgeLabelTest extends SparkTestSuite
{
  // Given a labeled set of edges,
  // verify that no vertex are labeled with two labels
  def vertexLabelUnique( edges: RDD[((Long,Long),Long)] ): Boolean = {
    edges.flatMap {
      case ((from,to),label) => Seq( (from,label), (to,label) )
    }
    .distinct
    .map {
      case (vertex,label) => (vertex,1)
    }
    .reduceByKey(_+_)
    .map {
      case (_,count) => count==1
    }
    .reduce(_&&_)
  }

  def testEdgeLabeling( a1: Array[(Long,Long)],
  a2: Array[((Long,Long),Long)] ) = {
    val edges2bLabeled = sc.parallelize(a1)
    val edgesLabeled = InfoFlow.labelEdges(edges2bLabeled)
    assert( vertexLabelUnique(edgesLabeled) )
    val expectedLabel = sc.parallelize(a2)
    assert( edgesLabeled.collect.sorted === expectedLabel.collect.sorted )
  }

  /***************************************************************************
   * Edge Labeling Tests
   ***************************************************************************/

  test("Empty graph") {
    val thrown = intercept[Exception] {
      val emptyArray = sc.parallelize( Array[(Long,Long)]() )
      InfoFlow.labelEdges(emptyArray)
    }
    assert( thrown.getMessage === "Empty RDD argument" )
  }

  test("Trivial graph") {
    testEdgeLabeling(
      Array( (1,2) ),
      Array( ((1,2),1) )
    )
  }

  test("Trivial disjoint graph") {
    testEdgeLabeling(
      Array( (1,2), (3,4) ),
      Array( ((1,2),1), ((3,4),3) )
    )
  }

  test("Trivial graph with self-connecting edge") {
    testEdgeLabeling(
      Array( (1,1) ),
      Array( ((1,1),1) )
    )
  }

  test("Trivial graph with loop") {
    testEdgeLabeling(
      Array( (1,2), (2,1) ),
      Array( ((1,2),1), ((2,1),1) )
    )
  }

  test("Loop size-4") {
    testEdgeLabeling(
      Array( (1,2), (2,3), (3,4), (4,1) ),
      Array( ((1,2),1), ((2,3),1), ((3,4),1), ((4,1),1) )
    )
  }

  test("Loop size-4 with duplicated edge") {
    testEdgeLabeling(
      Array( (1,2), (2,3), (3,4), (4,1), (4,1) ),
      Array( ((1,2),1), ((2,3),1), ((3,4),1), ((4,1),1), ((4,1),1) )
    )
  }

  test("Wrong sided loop") {
    testEdgeLabeling(
      Array(
        (1,2), (1,3), (2,3)
      ),
      Array(
        ((1,2),1), ((1,3),1), ((2,3),1)
      )
    )
  }

  test("Figure-8 loop") {
    testEdgeLabeling(
      Array( (1,2), (2,3), (3,1), (2,4), (4,2) ),
      Array( ((1,2),1), ((2,3),1), ((3,1),1), ((2,4),1), ((4,2),1) )
    )
  }

  test("O-o loop") {
    testEdgeLabeling(
      Array( (1,2), (2,3), (3,1), (3,2) ),
      Array( ((1,2),1), ((2,3),1), ((3,1),1), ((3,2),1) )
    )
  }

  test("T-shaped graph") {
    testEdgeLabeling(
      Array( (1,2), (2,3), (2,4) ),
      Array( ((1,2),1), ((2,3),1), ((2,4),1) )
    )
  }

  test("6-shaped graph") {
    testEdgeLabeling(
      Array( (1,2), (2,3), (3,1), (2,4) ),
      Array( ((1,2),1), ((2,3),1), ((3,1),1), ((2,4),1) )
    )
  }

  test("Small graph 1") {
    testEdgeLabeling(
      Array( (1,4), (7,9), (2,3), (3,5) ),
      Array( ((1,4),1), ((2,3),2), ((3,5),2), ((7,9),7) )
    )
  }

  test("Small graph 2") {
    testEdgeLabeling(
      Array( (3,7), (1,2), (3,9), (4,5), (2,5) ),
      Array( ((1,2),1), ((2,5),1), ((3,7),3), ((3,9),3), ((4,5),1) )
    )
  }

  test("Small all connected graph 1") {
    testEdgeLabeling(
      Array( (1,7), (2,3), (3,1), (4,5), (6,3), (7,5) ),
      Array( ((1,7),1), ((2,3),1), ((3,1),1), ((4,5),1), ((6,3),1), ((7,5),1) )
    )
  }

  test("Small all connected graph 2") {
    testEdgeLabeling(
      Array( (1,2), (2,3), (3,4), (4,5), (5,6), (6,7) ),
      Array( ((1,2),1), ((2,3),1), ((3,4),1), ((4,5),1), ((5,6),1), ((6,7),1) )
    )
  }

  test("Big graph 1") {
    testEdgeLabeling(
      Array(
        (4,32), (7,33), (2,10), (10,29), (10,31),
        (4,10), (13,31), (18,34), (20,21), (14,30),
        (10,18), (24,25), (10,33), (10,17), (14,19),
        (16,18), (26,30), (11,30), (1,9), (8,18),
        (10,22), (1,18), (25,35), (25,33), (5,28),
        (3,25)
      ),
      Array(
        ((4,32),1), ((7,33),1), ((2,10),1), ((10,29),1), ((10,31),1),
        ((4,10),1), ((13,31),1), ((18,34),1), ((20,21),20), ((14,30),11),
        ((10,18),1), ((24,25),1), ((10,33),1), ((10,17),1), ((14,19),11),
        ((16,18),1), ((26,30),11), ((11,30),11), ((1,9),1), ((8,18),1),
        ((10,22),1), ((1,18),1), ((25,35),1), ((25,33),1), ((5,28),5),
        ((3,25),1)
      )
    )
  }

  test("Big all connected graph") {
    testEdgeLabeling(
      Array(
        (4,32), (7,33), (2,10), (10,29), (10,31),
        (4,10), (13,31), (18,34),
        (10,18), (24,25), (10,33), (10,17),
        (16,18), (1,9), (8,18),
        (10,22), (1,18), (25,35), (25,33),
        (3,25)
      ),
      Array(
        ((4,32),1), ((7,33),1), ((2,10),1), ((10,29),1), ((10,31),1),
        ((4,10),1), ((13,31),1), ((18,34),1),
        ((10,18),1), ((24,25),1), ((10,33),1), ((10,17),1),
        ((16,18),1), ((1,9),1), ((8,18),1),
        ((10,22),1), ((1,18),1), ((25,35),1), ((25,33),1),
        ((3,25),1)
      )
    )
  }

  test("Weird graph") {
    testEdgeLabeling(
      Array(
        (269,2660), (269,59), (2660,59),
        (2660,6), (9,2660), (17,2660), (2660,35), (2660,3),
        (2660,5), (2,2660), (2660,40), (1,2660), (2660,68),
        (85,2660)
      ),
      Array(
        ((269,2660),1), ((269,59),1), ((2660,59),1),
        ((2660,6),1), ((9,2660),1), ((17,2660),1), ((2660,35),1), ((2660,3),1),
        ((2660,5),1), ((2,2660),1), ((2660,40),1), ((1,2660),1), ((2660,68),1),
        ((85,2660),1)
      )
    )
  }

  test("Random graph 1") {
    testEdgeLabeling(
      Array(
        (1,2), (1,1), (-4,6), (4,6), (2,3), (2,0), (2,1)
      ),
      Array(
        ((1,2),0), ((1,1),0), ((-4,6),-4), ((4,6),-4), ((2,3),0), ((2,0),0), ((2,1),0)
      )
    )
  }

  test("Random graph 2") {
    testEdgeLabeling(
      Array(
        (-1,8), (1,1), (-34,6), (4,6), (2,3), (2,0), (2,1)
      ),
      Array(
        ((-1,8),-1), ((1,1),0), ((-34,6),-34),
        ((4,6),-34), ((2,3),0), ((2,0),0), ((2,1),0)
      )
    )
  }
  test("Random graph 3") {
    testEdgeLabeling(
      Array(
        (3,2), (6,3), (-3,9), (1,9),
        (4,5), (3,7), (8,9), (42,59),
        (59,13), (12,-1), (3,6), (1,19), (14,2)
      ),
      Array(
        ((3,2),2), ((6,3),2), ((-3,9),-3), ((1,9),-3),
        ((4,5),4), ((3,7),2), ((8,9),-3), ((42,59),13),
        ((59,13),13), ((12,-1),-1), ((3,6),2), ((1,19),-3), ((14,2),2)
      )
    )
  }
}
