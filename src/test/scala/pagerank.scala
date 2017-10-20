/***************************************************************************
 * Test Suite for Page Rank algorithm
 ***************************************************************************/

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import org.scalactic.TolerantNumerics

class PageRankTest extends FunSuite with BeforeAndAfter
{

  /***************************************************************************
   * Comparisons of doubles has a tolerance of 1e-5
   ***************************************************************************/
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(1e-5)

  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
  var sc: SparkContext = _
  before {
    val conf = new SparkConf()
      .setAppName("InfoMap page rank tests")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "1")
    sc = new SparkContext(conf)
  }

  /***************************************************************************
   * Test Cases
   ***************************************************************************/

  test("Read trivial network with self loop in pure random walk") {
    val pj = new PajekFile(sc,"Nets/trivial-with-self-loop.net")
    val pr = new Nodes( pj, 1, 1e-2 )
    assert( pr.stoMat.sparse.collect.sorted ===
      Array( (1,(2,1)), (2,(2,1)) )
    )
    assert( pr.ergodicFreq.collect.sorted === Array( (1,0),(2,1) ) )
  }

  test("Read trivial network with self loop") {
    val pj = new PajekFile(sc,"Nets/trivial-with-self-loop.net")
    val pr = new Nodes( pj, 0.85, 1e-1 )
    val arrChk = new ArrayCheck(
      pr.ergodicFreq.collect,
      Array((1,0.075),(2,0.925))
    )
    if( !arrChk.eq ) fail( arrChk.msg )
  }

  test("Read trivial network with dangling node in pure random walk") {
    val pj = new PajekFile(sc,"Nets/trivial.net")
    val pr = new Nodes( pj, 1, 1e-3 )
    assert( pr.stoMat.sparse.collect.sorted ===
      Array( (1,(2,1)) )
    )
    assert( pr.stoMat.constCol.collect.sorted ===
      Array( (2,0.5) )
    )
    val arrChk = new ArrayCheck(
      pr.ergodicFreq.collect,
      Array((1,0.33333),(2,0.66667))
    )
    if( !arrChk.eq ) fail( arrChk.msg )
  }

  test("Read trivial network with dangling node") {
    val pj = new PajekFile(sc,"Nets/trivial.net")
    val pr = new Nodes( pj, 0.85, 1e-4 )
    val arrChk = new ArrayCheck(
      pr.ergodicFreq.collect,
      Array((1,0.35),(2,0.65))
    )
    if( !arrChk.eq ) fail( arrChk.msg )
  }

  test("Read one node network with no link") {
    val pj = new PajekFile(sc,"Nets/zero.net")
    val pr = new Nodes( pj, 0.85, 1e-1 )
    assert( pr.stoMat.sparse.collect.sorted === Array() )
    assert( pr.stoMat.constCol.collect.sorted ===
      Array( (1,1) )
    )
    val arrChk = new ArrayCheck( pr.ergodicFreq.collect, Array((1,1.0)) )
    if( !arrChk.eq ) fail( arrChk.msg )
  }

  test("Read Nets/small-asym.net") {
    val pj = new PajekFile(sc,"Nets/small-asym.net")
    val pr = new Nodes( pj, 0.85, 1e-0 )
    val freq = pr.ergodicFreq.collect.sorted.map{case(idx,x)=>x}
    assert( freq.sum === 1.0 )
  }

  test("Read Nets/kin.net") {
    val pj = new PajekFile(sc,"Nets/kin.net")
    val pr = new Nodes( pj, 0.85, 1e-0 )
    val freq = pr.ergodicFreq.collect.sorted.map{case(idx,x)=>x}
    assert( freq.sum === 1.0 )
  }

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
  after {
    if( sc != null )
      sc.stop
  }
}
