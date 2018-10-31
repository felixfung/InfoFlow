/***************************************************************************
 * Test Suite to output Json file for visualization
 ***************************************************************************/

import org.scalatest._
import Matchers._

import java.io.File
import java.io.FileReader
import java.io.BufferedReader
import org.apache.commons.io.FileUtils

class JsonGraphWriterTest extends SparkTestSuite
{
  val filename = "unittestfile.json"
  test("Json exportation of simple graph") {
    try {
      // produce json file
      val vertices = Array(
        (1L,("one",1L,0.0)),
        (2L,("two",1L,1.0)),
        (3L,("three",3L,1.0)),
        (4L,("four",4L,2.0))
      )
      val edges = Array(
        ((1L,2L),1.0),
        ((1L,3L),2.0),
        ((2L,3L),1.0)
      )
      val jsonGraph = JsonGraph( vertices, edges )
      JsonGraphWriter( filename, jsonGraph )

      // read and verify json file
      verifyFile(Array(
        "{",
        "\t\"nodes\": [",
        "\t\t{\"id\": \"1\", \"size\": \"0.0\", "
          +"\"name\": \"one\", \"group\": \"1\"},",
        "\t\t{\"id\": \"2\", \"size\": \"1.0\", "
          +"\"name\": \"two\", \"group\": \"1\"},",
        "\t\t{\"id\": \"3\", \"size\": \"1.0\", "
          +"\"name\": \"three\", \"group\": \"3\"},",
        "\t\t{\"id\": \"4\", \"size\": \"2.0\", "
          +"\"name\": \"four\", \"group\": \"4\"}",
        "\t],",
        "\t\"links\": [",
        "\t\t{\"source\": \"1\", \"target\": \"2\", \"value\": \"1.0\"},",
        "\t\t{\"source\": \"1\", \"target\": \"3\", \"value\": \"2.0\"},",
        "\t\t{\"source\": \"2\", \"target\": \"3\", \"value\": \"1.0\"}",
        "\t]",
        "}"
      ))
    }
    finally {
      // after everything is done, delete file
      val file = new File(filename)
      file.delete
    }
  }

  test("Json full exportation of simple graph") {
    try {
      // produce json file
      val graph = PajekReader( sc, "Nets/small-asym.net" )
      LogFile.saveFullJson( filename, "", graph )

      // read and verify json file
      verifyFile(Array(
        "{",
        "\t\"nodes\": [",
        "\t\t{\"id\": \"-3\", \"size\": \"0.0\", "
          +"\"name\": \"\", \"group\": \"3\"},",
        "\t\t{\"id\": \"-2\", \"size\": \"0.0\", "
          +"\"name\": \"\", \"group\": \"2\"},",
        "\t\t{\"id\": \"-1\", \"size\": \"0.0\", "
          +"\"name\": \"\", \"group\": \"1\"},",
        "\t\t{\"id\": \"1\", \"size\": \"1.0\", "
          +"\"name\": \"1\", \"group\": \"1\"},",
        "\t\t{\"id\": \"2\", \"size\": \"1.0\", "
          +"\"name\": \"2\", \"group\": \"2\"},",
        "\t\t{\"id\": \"3\", \"size\": \"1.0\", "
          +"\"name\": \"3\", \"group\": \"3\"}",
        "\t],",
        "\t\"links\": [",
        "\t\t{\"source\": \"1\", \"target\": \"2\", \"value\": \"1.0\"},",
        "\t\t{\"source\": \"2\", \"target\": \"1\", \"value\": \"1.0\"}",
        "\t]",
        "}"
      ))
    }
    finally {
      // after everything is done, delete file
      val file = new File(filename)
      file.delete
    }
  }

  test("Json reduced exportation of simple graph") {
    try {
      // produce json file
      val graph = PajekReader( sc, "Nets/small-asym.net" )
      val net0 = Network.init( graph, 0.85 )
      val infoMap = new InfoMap
      val logFile = new LogFile(sc,"","","","","",false)
      val (_,net1) = infoMap( graph, net0, logFile )
      LogFile.saveReducedJson( filename, "", net1 )

      // read and verify json file
      verifyFile(Array(
        "{",
        "\t\"nodes\": [",
        "\t\t{\"id\": \"1\", \"size\": \"4.0\", "
          +"\"name\": \"\", \"group\": \"1\"},",
        "\t\t{\"id\": \"3\", \"size\": \"1.0\", "
          +"\"name\": \"\", \"group\": \"3\"}",
        "\t]",
        "}"
      ))
    }
    finally {
      // after everything is done, delete file
      val file = new File(filename)
      //file.delete
    }
  }

  private def verifyFile( fileLines: Array[String] ): Unit = {
    val reader = new BufferedReader( new FileReader(filename) )
    var line = reader.readLine
    var lineIdx = 0
    var looping = true
    while(looping) {
      assert( line === fileLines(lineIdx) )
      lineIdx += 1
      line = reader.readLine
      looping = ( line != null )
    }
    reader.close
  }
}
