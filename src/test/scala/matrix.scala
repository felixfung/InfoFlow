/***************************************************************************
 * Test Suite for Matrix algorithm
 ***************************************************************************/

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import org.scalactic.TolerantNumerics

class MatrixTest extends FunSuite with BeforeAndAfter
{

  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
  var sc: SparkContext = _
  before {
    val conf = new SparkConf()
      .setAppName("InfoMap matrix tests")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "1")
    sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
  }

  /***************************************************************************
   * Test Cases
   ***************************************************************************/
  test("Calculate 1x1 matrix and vector") {
    val cm = new Matrix(sc.parallelize(List( (1,(1,1)) )), sc.parallelize(List()))
    val ar = sc.parallelize(List[(Int,Double)]( (1,-2) ))
    assert( (cm*ar).collect.sorted === ar.collect.sorted )
  }
  // careful that the matrix format is (from,(to,entry))
  // where "from" is the column index, "to" is the row index

  test("Calculate 2x2 identity matrix and vector") {
    val cm = new Matrix(sc.parallelize(List(
      (1,(1,1)),
      (2,(2,1))
    )), sc.parallelize(List()))
    val ar = sc.parallelize(List[(Int,Double)]( (1,-2), (2,7) ))
    assert( (cm*ar).collect.sorted === ar.collect.sorted )
  }

  test("Calculate 2x2 permutative matrix and vector") {
    val cm = new Matrix(sc.parallelize(List(
      (2,(1,1)),
      (1,(2,1))
    )), sc.parallelize(List()))
    val ar = sc.parallelize(List[(Int,Double)]( (1,-2), (2,7) ))
    val pr = sc.parallelize(List[(Int,Double)]( (1,7), (2,-2) ))
    assert( (cm*ar).collect.sorted === pr.collect.sorted )
  }

  test("Calculate 2x2 nontrivial matrix and vector") {
    val cm = new Matrix(sc.parallelize(List(
      (1,(1,1)), (2,(1,2)),
      (1,(2,3)), (2,(2,4))
    )), sc.parallelize(List()))
    val ar = sc.parallelize(List[(Int,Double)]( (1,5), (2,6) ))
    val pr = sc.parallelize(List[(Int,Double)]( (1,17), (2,39) ))
    assert( (cm*ar).collect.sorted === pr.collect.sorted )
  }

  test("Calculate 3x3 \"hanging\" matrix and vector") {
    val cm = new Matrix(sc.parallelize(List(
      (2,(1,1)),
      (1,(2,2)),
      (2,(2,3))
    )), sc.parallelize(List()))
    val ar = sc.parallelize(List[(Int,Double)]( (1,1), (2,2), (3,3) ))
    val pr = sc.parallelize(List[(Int,Double)]( (1,2), (2,8), (3,0) ))
    assert( (cm*ar).collect.sorted === pr.collect.sorted )
  }

  test("Calculate 3x3 permutative matrix and vector") {
    val cm = new Matrix(sc.parallelize(List(
      (2,(1,1)),
      (1,(2,1)),
      (3,(3,1))
    )), sc.parallelize(List()))
    val ar = sc.parallelize(List[(Int,Double)]( (1,1), (2,2), (3,3) ))
    val pr = sc.parallelize(List[(Int,Double)]( (1,2), (2,1), (3,3) ))
    assert( (cm*ar).collect.sorted === pr.collect.sorted )
  }

  test("Calculate 3x3 nontrivial matrix and vector") {
    val cm = new Matrix(sc.parallelize(List(
      (1,(1,-9)),
      (1,(2,1)),
      (2,(2,11)),
      (3,(2,-2)),
      (2,(3,3))
    )), sc.parallelize(List()))
    val ar = sc.parallelize(List[(Int,Double)]( (1,7), (2,0), (3,-2) ))
    val pr = sc.parallelize(List[(Int,Double)]( (1,-63), (2,11), (3,0) ))
    assert( (cm*ar).collect.sorted === pr.collect.sorted )
  }

  test("Calculate 4x4 nontrivial \"hanging\" matrix and vector") {
    val cm = new Matrix(sc.parallelize(List(
      (2,(1,-2)),
      (3,(1,3)),
      (1,(2,1)),
      (3,(2,4)),
      (4,(2,2))
    )), sc.parallelize(List()))
    val ar = sc.parallelize(List[(Int,Double)]( (1,-4), (2,2), (3,8), (4,0) ))
    val pr = sc.parallelize(List[(Int,Double)]( (1,20), (2,28), (3,0), (4,0) ))
    assert( (cm*ar).collect.sorted === pr.collect.sorted )
  }

  test("Calculate 1x1 matrix with constant column optimization") {
    val cm = new Matrix(sc.parallelize(List()),
      sc.parallelize(List[(Int,Double)]((1,1.0))))
    val ar = sc.parallelize(List[(Int,Double)]( (1,0.7) ))
    assert( (cm*ar).collect.sorted === ar.collect.sorted )
  }

  test("Calculate 2x2 matrix with constant column optimization") {
    val cm = new Matrix(sc.parallelize(List(
      (1,(2,1))
    )), sc.parallelize(List[(Int,Double)]((2,0.5))))
    val ar = sc.parallelize(List[(Int,Double)]( (1,0.5), (2,0.5) ))
    val pr = sc.parallelize(List[(Int,Double)]( (1,0.25), (2,0.75) ))
    assert( (cm*ar).collect.sorted === pr.collect.sorted )
  }

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
  after {
    if( sc != null )
      sc.stop
  }
}
