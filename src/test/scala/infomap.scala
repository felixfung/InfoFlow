/***************************************************************************
 * Test Suite for InfoMap merge algorithm
 ***************************************************************************/

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class InfoMapTest extends FunSuite with BeforeAndAfter
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
   * Test Cases
   ***************************************************************************/

  /*test("Test trivial network") {
    val logFile = new LogFile("Trivial Output InfoMap")
    val infoMap = MergeAlgoTest( sc, new InfoMap(logFile) )
    infoMap(
      "Nets/trivial.net",
      0, 1.46, 1.45,
      Array(
        """\(m0(1),([0-9])\)""",
        """\(m0(2),([0-9])\)"""
      )
    )
  }

  test("Test small network") {
    val logFile = new LogFile("Small Output InfoMap")
    val infoMap = MergeAlgoTest( sc, new InfoMap(logFile) )
    infoMap(
      "Nets/small.net",
      2, 4.00, 1.58,
      Array(
        """\(([12]),([0-9])\)""",
        """\(([34]),([0-9])\)"""
      )
    )
  }

  test("Test small asymmetric network") {
    val infoMap = MergeAlgoTest( sc, new InfoMap("Small Asym Output InfoMap") )
    infoMap(
      "Nets/small-asym.net",
      1, 2.92, 1.38,
      Array(
        """\(([12]),([0-9])\)""",
        """\((3),([0-9])\)"""
      )
    )
  }

  test("Read simple test network") {
    val infoMap = MergeAlgoTest( sc, new InfoMap("Simple Output InfoMap") )
    infoMap(
      "Nets/simple.net",
      4, 4.8, 2.38,
      Array(
        """\(([123]),([0-9])\)""",
        """\(([456]),([0-9])\)"""
      )
    )
  }

  test("Reproduce Rosvall and Bergstrom 2008 result") {
    val infoMap = MergeAlgoTest( sc, new InfoMap("Rosvall Output InfoMap") )
    infoMap(
      "Nets/rosvall.net",
      21, 6.55, 3.51,
      Array(
        """\(red([01]+),([0-9]+)\)""",
        """\(orange([01]+),([0-9]+)\)""",
        """\(green([01]+),([0-9]+)\)""",
        """\(blue([01]+),([0-9]+)\)"""
      )
    )
  }

  test("InfoMap vs modularity test 1") {
    val infoMap = MergeAlgoTest( sc, new InfoMap("VS1 Output InfoMap") )
    infoMap(
      "Nets/infoflow-vs-modularity1.net",
      12, 5.99, 3.43,
      Array(
        """\(red([0-9]*),([0-9]*)\)""",
        """\(green([0-9]*),([0-9]*)\)""",
        """\(blue([0-9]*),([0-9]*)\)""",
        """\(yellow([0-9]*),([0-9]*)\)"""
      )
    )
  }

  test("InfoMap vs modularity test 2") {
    val infoMap = MergeAlgoTest( sc, new InfoMap("VS2 Output InfoMap") )
    infoMap(
      "Nets/infoflow-vs-modularity2.net",
      0, 2.69, 2.68,
      Array(
      )
    )
  }*/

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
  after {
    if( sc != null )
      sc.stop
  }
}
