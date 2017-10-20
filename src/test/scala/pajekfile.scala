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

  test("Read trivial networks") {
    val pajek = new PajekFile(sc,"Nets/trivial.net")
    assert( pajek.n === 2 )
    assert( pajek.names.collect() === Array((1,"m01"),(2,"m02")) )
    assert( pajek.weights.collect() === Array( (1,2),(2,1)) )
    assert( pajek.sparseMat.collect() === Array( (1,(2,2)) ) )
  }

  test("Read Nets/kin.net") {
    val pajek = new PajekFile(sc,"Nets/kin.net")
    assert( pajek.n === 20 )
    assert( pajek.names.collect()(7) === (8,"f08") )
    assert( pajek.weights.collect()(16) === (17,0.5) )
    val correctEntries =
      Array(
        (6,(3,1)),
        (6,(7,1)),
        (6,(8,1)),
        (6,(9,1)),
        (9,(1,1)),
        (9,(16,1)),
        (11,(1,1)),
        (11,(12,1)),
        (13,(2,1)),
        (13,(14,1)),
        (13,(15,1)),
        (16,(1,1)),
        (16,(17,1)),
        (19,(1,1)),
        (19,(20,1))
      )
    assert( pajek.sparseMat.collect.sorted === correctEntries )
  }

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
  after {
    if( sc != null )
      sc.stop
  }
}
