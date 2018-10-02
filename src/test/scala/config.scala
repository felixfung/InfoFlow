/***************************************************************************
 * Test Suite for parsing config file
 ***************************************************************************/

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import java.io._

class ConfigTest extends FunSuite with BeforeAndAfter
{

  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
  var sc: SparkContext = _
  before {
    val conf = new SparkConf()
      .setAppName("InfoFlow config file parsing tests")
      .setMaster("local[*]")
    sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
  }

  /***************************************************************************
   * Test Cases
   ***************************************************************************/
  test("Parse sample config file") {
    try {
      // produce config file
      val writer = new PrintWriter(new File("unittestconfig.json"))
      writer.write("{\n")
      writer.write("\t\"Master\": \"local[*]\",\n")
      writer.write("\t\"Pajek\": \"Nets/rosvall.net\",\n")
      writer.write("\t\"Algo\": \"InfoFlow\",\n")
      writer.write("\t\"damping\": \"0.85\",\n")
      writer.write("\t\"logDir\": \"Football\",\n")
      writer.write("\t\"logWriteLog\": \"true\",\n")
      writer.write("\t\"logRddText\": \"true\",\n")
      writer.write("\t\"logRddJSon\": \"1\",\n")
      writer.write("\t\"logSteps\": \"false\"\n")
      writer.write("}")
      writer.close

      // verify parsing
      val config = new Config("unittestconfig.json")
      assert( config.master === "local[*]" )
      assert( config.pajekFile === "Nets/rosvall.net" )
      assert( config.dampingFactor === 0.85 )
      assert( config.mergeAlgo === "InfoFlow" )
      assert( config.logWriteLog === true )
      assert( config.rddText === true )
      assert( config.rddJSon === 1 )
      assert( config.logSteps === false )
    }
    finally {
      // delete config file
      val file = new File("unittestconfig.json")
      file.delete
    }
  }

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
  after {
    if( sc != null )
      sc.stop
  }
}
