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

  test("Read trivial networks") {
    val pajek = new PajekFile(sc,"Nets/trivial.net")
    assert( pajek.n === 2 )
    assert( pajek.names.collect() === Array((1,"m01"),(2,"m02")) )
    assert( pajek.sparseMat.collect() === Array( (1,(2,2)) ) )
  }

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
  after {
    if( sc != null )
      sc.stop
  }
}
