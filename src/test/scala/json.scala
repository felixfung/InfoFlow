/***************************************************************************
 * Test Suite for Matrix algorithm
 ***************************************************************************/

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import org.scalatest._
import Matchers._

import java.io.File
import java.io.FileReader
import java.io.BufferedReader
import org.apache.commons.io.FileUtils

class JSonTest extends FunSuite with BeforeAndAfter
{

  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
  var sc: SparkContext = _
  before {
    val conf = new SparkConf()
      .setAppName("InfoFlow JSon exportation tests")
      .setMaster("local[*]")
    sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
  }

  /***************************************************************************
   * Test Cases
   ***************************************************************************/
  ignore("JSon exportation of trivial graph") {
    try {
      // export JSon file
      val pj = new PajekFile( sc, "Nets/trivial.net" )
      val nodes = new Nodes( pj, 0.85, 1e-3 )
      val partition = Partition.init(nodes)
      partition.saveJSon("unittestfile.json")

      // read and verify JSon file
      val reader = new BufferedReader( new FileReader("unittestfile.json") )
      val chkArray = Array[String](
        "{",
        "\t\"nodes\": [",
        "\t\t{\"id\": \"1\", \"size\": \"1\", \"name\": \"m01\", \"group\": \"1\"},",
        "\t\t{\"id\": \"2\", \"size\": \"1\", \"name\": \"m02\", \"group\": \"2\"},",
        "\t\t{\"id\": \"-1\", \"size\": \"0\", \"name\": \"\", \"group\": \"1\"},",
        "\t\t{\"id\": \"-2\", \"size\": \"0\", \"name\": \"\", \"group\": \"2\"}",
        "\t],",
        "\t\"links\": [",
        "\t\t{\"source\": \"1\", \"target\": \"2\", \"value\": 0.351035921701126}",
        "\t]",
        "}"
      )
      var line = reader.readLine
      var lineIdx = 0
      var looping = true
      while(looping) {
        assert( line === chkArray(lineIdx) )
        lineIdx += 1
        line = reader.readLine
        looping = ( line != null )
      }
      reader.close
    }
    finally {
      // after everything is done, delete file
      val file = new File("unittestfile.json")
      file.delete
    }
  }

  ignore("Reduced JSon exportation of Rosvall example graph") {
    try {
      // export JSon file
      val pj = new PajekFile( sc, "Nets/rosvall.net" )
      val nodes = new Nodes( pj, 0.85, 1e-3 )
      val initPartition = Partition.init(nodes)
      val infoFlow = new InfoFlow
      val logFile = new LogFile("unittestlog",false,false,0,false,false)
      val finalPartition = infoFlow( initPartition, logFile )
      finalPartition.saveReduceJSon("unittestfile.json")

      // read and verify JSon file
      val reader = new BufferedReader( new FileReader("unittestfile.json") )
      val chkArray = Array[String](
        "\\{",
        "\t\"nodes\": \\[",
        "\t\t\\{\"id\": \"2\", \"size\": \"6\", \"name\": \"red.*\", \"group\": \"2\"\\},",
        "\t\t\\{\"id\": \"7\", \"size\": \"7\", \"name\": \"orange.*\", \"group\": \"7\"\\},",
        "\t\t\\{\"id\": \"17\", \"size\": \"8\", \"name\": \"green.*\", \"group\": \"17\"\\},",
        "\t\t\\{\"id\": \"23\", \"size\": \"4\", \"name\": \"blue.*\", \"group\": \"23\"\\}",
        "\t\\],",
        "\t\"links\": \\[",
		"\t\t\\{\"source\": \"2\", \"target\": \"7\", \"value\": 0.0022363558809370297\\},",
		"\t\t\\{\"source\": \"7\", \"target\": \"17\", \"value\": 0.0189285421768626\\},",
		"\t\t\\{\"source\": \"7\", \"target\": \"23\", \"value\": 0.006803128149374325\\},",
		"\t\t\\{\"source\": \"17\", \"target\": \"23\", \"value\": 0.008674494124000919\\}",
        "\t\\]",
        "\\}"
      )
      var line = reader.readLine
      var lineIdx = 0
      var looping = true
      while(looping) {
        line should fullyMatch regex( chkArray(lineIdx) )
        lineIdx += 1
        line = reader.readLine
        looping = ( line != null )
      }
      reader.close
    }
    finally {
      // after everything is done, delete file
      val file = new File("unittestfile.json")
      file.delete
      FileUtils.deleteDirectory( new File("unittestlog") )
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
