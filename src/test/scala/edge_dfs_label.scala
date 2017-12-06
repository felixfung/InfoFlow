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

    // generate iterables of components from edges1
    val components1 = edges1.flatMap {
      case ((from,to),label) => Seq( (label,from), (label,to) )
    }
    .groupByKey
    .map {
      case (label,vertices) => vertices
    }
    .collect
    .sorted

    // generate iterables of components from edges2
    val components2 = edges2.flatMap {
      case ((from,to),label) => Seq( (label,from), (label,to) )
    }
    .groupByKey
    .map {
      case (label,vertices) => vertices
    }
    .collect
    .sorted

    // compare the two iterables
    if( components1.length != components2.length ) false
    else {
      var res = true
      for( i <- 0 to components1.length-1 if res ) {
        val array1 = components1(i).toArray.sorted
        val array2 = components2(i).toArray.sorted
        var res = {
          if( array1.size != array2.size ) false
          else {
            var eq = true
            for( j <- 0 to components1(i).size-1 if eq ) {
              eq = array1(j) == array2(j)
            }
            eq
          }
        }
        res
      }
      res
    }
  }

  /***************************************************************************
   * Edge Labeling Tests
   ***************************************************************************/

  test("Label edges 0") {
    val edges2bLabeled =
      sc.parallelize(Array( (1,2) ))
    val edgesLabeled = InfoFlow.labelEdges(edges2bLabeled)
    assert( vertexLabelUnique(edgesLabeled) )
    val expectedLabel = sc.parallelize(Array( ((1,2),1) ) )
    assert( connectedComponentsEq( edgesLabeled, expectedLabel ) )
  }

  test("Label edges 1") {
    val edges2bLabeled =
      sc.parallelize(Array( (3,7), (1,2), (3,9), (4,5), (2,5) ))
    val edgesLabeled = InfoFlow.labelEdges(edges2bLabeled)
    assert( vertexLabelUnique(edgesLabeled) )
    val expectedLabel = sc.parallelize(Array( ((1,2),5), ((2,5),5), ((3,7),3), ((3,9),3), ((4,5),5) ) )
    assert( connectedComponentsEq( edgesLabeled, expectedLabel ) )
  }

  test("Label edges 2") {
    val edges2bLabeled =
      sc.parallelize(Array( (1,7), (2,3), (3,1), (4,5), (6,3), (7,5) ))
    val edgesLabeled = InfoFlow.labelEdges(edges2bLabeled)
    val expectedLabel = sc.parallelize(
      Array( ((1,7),1), ((2,3),1), ((3,1),1), ((4,5),1), ((6,3),1), ((7,5),1) )
    )
    assert( connectedComponentsEq( edgesLabeled, expectedLabel ) )
  }

  test("Label edges 3") {
    val edges2bLabeled =
      sc.parallelize(Array( (1,4), (7,9), (2,3), (3,5) ))
    val edgesLabeled = InfoFlow.labelEdges(edges2bLabeled)
    assert( vertexLabelUnique(edgesLabeled) )
    val expectedLabel = sc.parallelize(
      Array( ((1,4),1), ((2,3),3), ((3,5),3), ((7,9),7) )
    )
    assert( connectedComponentsEq( edgesLabeled, expectedLabel ) )
  }

  test("Label edges 4") {
    val edges2bLabeled =
      sc.parallelize(Array( (1,2), (2,3), (3,4), (4,5), (5,6), (6,7) ))
    val edgesLabeled = InfoFlow.labelEdges(edges2bLabeled)
    assert( vertexLabelUnique(edgesLabeled) )
    val expectedLabel = sc.parallelize(
      Array( ((1,2),1), ((2,3),1), ((3,4),1), ((4,5),1), ((5,6),1), ((6,7),1) )
    )
    assert( connectedComponentsEq( edgesLabeled, expectedLabel ) )
  }

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
  after {
    if( sc != null )
      sc.stop
  }
}
