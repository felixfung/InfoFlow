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
   * Edge Labeling Tests
   ***************************************************************************/

  test("Label edges 0") {
    val edges2bLabeled =
      sc.parallelize(Array( (1,2) ))
    val edgesLabeled = InfoFlow.labelEdges(edges2bLabeled)
    assert( edgesLabeled.collect.sorted ===
      Array( ((1,2),1) )
    )
  }

  test("Label edges 1") {
    val edges2bLabeled =
      sc.parallelize(Array( (3,7), (1,2), (3,9), (4,5), (2,5) ))
    val edgesLabeled = InfoFlow.labelEdges(edges2bLabeled)
    assert( edgesLabeled.collect.sorted ===
      Array( ((1,2),2), ((2,5),2), ((3,7),3), ((3,9),3), ((4,5),2) )
    )
  }

  test("Label edges 2") {
    val edges2bLabeled =
      sc.parallelize(Array( (1,7), (2,3), (3,1), (4,5), (6,3), (7,5) ))
    val edgesLabeled = InfoFlow.labelEdges(edges2bLabeled)
    assert( edgesLabeled.collect.sorted ===
      Array( ((1,7),3), ((2,3),3), ((3,1),3), ((4,5),5), ((6,3),3), ((7,5),5) )
    )
  }

  test("Label edges 3") {
    val edges2bLabeled =
      sc.parallelize(Array( (1,4), (7,9), (2,3), (3,5) ))
    val edgesLabeled = InfoFlow.labelEdges(edges2bLabeled)
    assert( edgesLabeled.collect.sorted ===
      Array( ((1,4),1), ((2,3),3), ((3,5),3), ((7,9),7) )
    )
  }

  test("Label edges 4") {
    val edges2bLabeled =
      sc.parallelize(Array( (1,2), (2,3), (3,4), (4,5), (5,6), (6,7) ))
    val edgesLabeled = InfoFlow.labelEdges(edges2bLabeled)
    assert( edgesLabeled.collect.sorted ===
      Array( ((1,2),2), ((2,3),2), ((3,4),2), ((4,5),2), ((5,6),2), ((6,7),2) )
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
