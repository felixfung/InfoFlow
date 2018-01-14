/***************************************************************************
 * Test Suite for reading Pajek files
 ***************************************************************************/

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import org.scalactic.TolerantNumerics

class PajekFileTest extends FunSuite with BeforeAndAfter
{

  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
  var sc: SparkContext = _
  before {
    val conf = new SparkConf()
      .setAppName("InfoMap Pajek file tests")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "1")
    sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
  }

  /***************************************************************************
   * Test Cases
   ***************************************************************************/
  test("Throw error when reading wrong file") {
    val thrown = intercept[Exception] {
      val dummy = new PajekFile(sc,"Nets/dummy")
    }
    assert( thrown.getMessage === "Cannot open file Nets/dummy" )
  }

  test("Read trivial network with comment") {
    val pajek = new PajekFile(sc,"Nets/zero.net")
    assert( pajek.n === 1 )
    assert( pajek.names.collect === Array((1,"v1")) )
    assert( pajek.sparseMat.collect === Array() )
  }

  test("Read trivial networks") {
    val pajek = new PajekFile(sc,"Nets/trivial.net")
    assert( pajek.n === 2 )
    assert( pajek.names.collect === Array((1,"m01"),(2,"m02")) )
    assert( pajek.sparseMat.collect === Array( (1,(2,2)) ) )
  }

  test("Read trivial networks with self loop") {
    val pajek = new PajekFile(sc,"Nets/trivial-with-self-loop.net")
    assert( pajek.n === 2 )
    assert( pajek.names.collect === Array((1,"m01"),(2,"m02")) )
    assert( pajek.sparseMat.collect.sorted === Array( (1,(2,2)), (2,(2,1)) ) )
  }

  test("Read simple network") {
    val pajek = new PajekFile(sc,"Nets/simple.net")
    assert( pajek.n === 6 )
    assert( pajek.names.collect.sorted ===
      Array(
        (1,"1"),
        (2,"2"),
        (3,"3"),
        (4,"4"),
        (5,"5"),
        (6,"6")
      )
    )
    assert( pajek.sparseMat.collect.sorted ===
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
    val pajek = new PajekFile(sc,"Nets/edge-test.net")
    assert( pajek.n === 6 )
    assert( pajek.names.collect === Array(
      (1,"1"),(2,"2"),(3,"3"),(4,"4"),(5,"5"),(6,"6")
    ) )
    assert( pajek.sparseMat.collect.sorted === Array(
      (1,(2,1)), (1,(3,1)), (1,(4,1)),
      (2,(1,1)), (2,(2,1)), (2,(6,1))
    ) )
  }

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
  after {
    if( sc != null )
      sc.stop
  }
}
