/***************************************************************************
 * Test Suite for testing the DFS edge labeling code
 ***************************************************************************/

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.io.Source

class EdgeLabelTest extends FunSuite with BeforeAndAfter
{

  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
  var sc: SparkContext = _
  before {
    val conf = new SparkConf()
      .setAppName("InfoMap partition tests")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "1")
    sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
  }

  /***************************************************************************
   * Given a labeled set of edges,
   * verify that no vertex are labeled with two labels
   ***************************************************************************/
  def vertexLabelUnique( edges: RDD[((Int,Int),Int)] ): Boolean = {
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
  /***************************************************************************
    * Returns whether two sets of edges are labelled identically,
    * According to connected components.
    * The actual labels can be different
    * e.g. ((1,2),1) == ((1,2),2),
    * since the connected components are the same according to the labels
    ***************************************************************************/
  def connectedComponentsEq(
    edges1: RDD[((Int,Int),Int)],
    edges2: RDD[((Int,Int),Int)]
  ): Boolean = {

    def labeledEdges22DArray( edges: RDD[((Int,Int),Int)] ) = {
      def tupleComp( t1: Array[(Int,Int)], t2: Array[(Int,Int)] ) =
        t1(0)._1 <= t2(0)._1
      edges.map {
        case ((from,to),label) => (label,(from,to))
      }
      .groupByKey
      .map {
        case (label,edgesIter) => edgesIter.toArray.sorted
      }
      .collect
      .sortWith( tupleComp )
    }

    val components1 = labeledEdges22DArray(edges1)
    val components2 = labeledEdges22DArray(edges2)

    if( components1.length != components2.length ) {
      false
    }
    else {
      var res = true
      for( i <- 0 to components1.length-1 if res ) {
        if( components1(i).size != components2(i).size )
          res = false
        else {
          for( j <- 0 to components1(i).size-1 if res )
            res = ( components1(i)(j) == components2(i)(j) )
        }
      }
      res
    }
  }

  def testEdgeLabeling( a1: Array[(Int,Int)], a2: Array[((Int,Int),Int)] ) = {
    val edges2bLabeled = sc.parallelize(a1)
    val edgesLabeled = InfoFlow.labelEdges(edges2bLabeled)
    edgesLabeled.collect.foreach(print)
    println("")
    assert( vertexLabelUnique(edgesLabeled) )
    val expectedLabel = sc.parallelize(a2)
    assert( connectedComponentsEq( edgesLabeled, expectedLabel ) )
  }

  /***************************************************************************
   * Edge Labeling Tests
   ***************************************************************************/

  test("Trivial graph") {
    testEdgeLabeling(
      Array( (1,2) ),
      Array( ((1,2),2) )
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

  test("T-shaped graph") {
    testEdgeLabeling(
      Array( (1,2), (2,3), (2,4) ),
      Array( ((1,2),1), ((2,3),1), ((2,4),1) )
    )
  }

  test("6-shaped graph") {
    testEdgeLabeling(
      Array( (1,2), (2,1), (1,3) ),
      Array( ((1,2),1), ((2,1),1), ((1,3),1) )
    )
  }

  test("Small graph 1") {
    testEdgeLabeling(
      Array( (1,4), (7,9), (2,3), (3,5) ),
      Array( ((1,4),1), ((2,3),3), ((3,5),3), ((7,9),7) )
    )
  }

  test("Small graph 2") {
    testEdgeLabeling(
      Array( (3,7), (1,2), (3,9), (4,5), (2,5) ),
      Array( ((1,2),5), ((2,5),5), ((3,7),3), ((3,9),3), ((4,5),5) )
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
        ((4,32),10), ((7,33),10), ((2,10),10), ((10,29),10), ((10,31),10),
        ((4,10),10), ((13,31),10), ((18,34),10), ((20,21),20), ((14,30),30),
        ((10,18),10), ((24,25),10), ((10,33),10), ((10,17),10), ((14,19),30),
        ((16,18),10), ((26,30),30), ((11,30),30), ((1,9),10), ((8,18),10),
        ((10,22),10), ((1,18),10), ((25,35),10), ((25,33),10), ((5,28),5),
        ((3,25),10)
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

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
  after {
    if( sc != null )
      sc.stop
  }
}