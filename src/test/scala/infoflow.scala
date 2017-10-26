/***************************************************************************
 * Test Suite for InfoFlow merge algorithm
 ***************************************************************************/

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class InfoFlowTest extends FunSuite with BeforeAndAfter
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

  test("Test trivial network") {
    val infoFlow = MergeAlgoTest( sc, new InfoFlow )
    infoFlow(
      "Nets/trivial.net", "Trivial Output InfoFlow",
      0, 1.46, 1.45,
      Array(
        """\(m0(1),([0-9])\)""",
        """\(m0(2),([0-9])\)"""
      )
    )
  }

  /*test("Test small network") {
    val infoFlow = MergeAlgoTest( sc, new InfoFlow("Small Output InfoFlow") )
    infoFlow(
      "Nets/small.net",
      1, 4.00, 1.58,
      Array(
        """\(([12]),([0-9])\)""",
        """\(([34]),([0-9])\)"""
      )
    )
  }

  test("Test small asymmetric network") {
    val infoFlow = MergeAlgoTest( sc, new InfoFlow("Small Asym Output InfoFlow") )
    infoFlow(
      "Nets/small-asym.net",
      1, 2.92, 1.38,
      Array(
        """\(([12]),([0-9])\)""",
        """\((3),([0-9])\)"""
      )
    )
  }

  test("Read simple test network") {
    val infoFlow = MergeAlgoTest( sc, new InfoFlow("Simple Output InfoFlow") )
    infoFlow(
      "Nets/simple.net",
      1, 4.58, 2.38,
      Array(
        """\(([123]),([0-9])\)""",
        """\(([456]),([0-9])\)"""
      )
    )
  }

  test("Reproduce Rosvall and Bergstrom 2008 result") {
    val infoFlow = MergeAlgoTest( sc, new InfoFlow("Rosvall Output InfoFlow") )
    infoFlow(
      "Nets/rosvall.net",
      2, 6.55, 3.51,
      Array(
        """\(red([01]+),([0-9]+)\)""",
        """\(orange([01]+),([0-9]+)\)""",
        """\(green([01]+),([0-9]+)\)""",
        """\(blue([01]+),([0-9]+)\)"""
      )
    )
  }

  test("InfoFlow vs modularity test 1") {
    val infoFlow = MergeAlgoTest( sc, new InfoFlow("VS1 Output InfoFlow") )
    infoFlow(
      "Nets/infoflow-vs-modularity1.net",
      2, 5.99, 3.43,
      Array(
        """\(red([0-9]*),([0-9]*)\)""",
        """\(green([0-9]*),([0-9]*)\)""",
        """\(blue([0-9]*),([0-9]*)\)""",
        """\(yellow([0-9]*),([0-9]*)\)"""
      )
    )
  }

  test("InfoFlow vs modularity test 2") {
    val infoFlow = MergeAlgoTest( sc, new InfoFlow("VS2 Output InfoFlow") )
    infoFlow(
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
