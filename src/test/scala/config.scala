import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import java.io._

class ConfigFileTest extends FunSuite
{
  test("Parse simple config file") {
    val filename = "unittestconfig.json"
    try {
      // produce config file
      val writer = new PrintWriter(new File(filename))
      writer.write("{\n")
      writer.write("\t\"Master\": \"local[*]\",\n")
      writer.write("\t\"Graph\": \"Nets/rosvall.net\",\n")
      writer.write("\t\"Algo\": \"InfoFlow\",\n")
      writer.write("\t\"tele\": \"0.15\",\n")
      writer.write("\t\"log\": {\n")
      writer.write("\t\t\"log path\": \"Output/log.txt\",\n")
      writer.write("\t\t\"Parquet path\": \"\",\n")
      writer.write("\t\t\"RDD path\": \"\",\n")
      writer.write("\t\t\"Json path\": \"Output/graph.json\",\n")
      writer.write("\t\t\"save partition\": \"true\",\n")
      writer.write("\t\t\"save name\": \"true\",\n")
      writer.write("\t\t\"debug\": \"true\"\n")
      writer.write("\t}\n")
      writer.write("}\n")
      writer.close

      // verify parsing
      val configFile = ConfigFile(filename)
      assert( configFile.master === "local[*]" )
      assert( configFile.graphFile === "Nets/rosvall.net" )
      assert( configFile.algorithm === "InfoFlow" )
      assert( configFile.tele === 0.15 )
      assert( configFile.logFile.pathLog === "Output/log.txt" )
      assert( configFile.logFile.pathParquet === "" )
      assert( configFile.logFile.pathRDD === "" )
      assert( configFile.logFile.pathJson === "Output/graph.json" )
      assert( configFile.logFile.savePartition === true )
      assert( configFile.logFile.saveName === true )
      assert( configFile.logFile.debug === true )
    }
    finally {
      val file = new File(filename)
      file.delete
    }
  }
}
