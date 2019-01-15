/*****************************************************************************
 * verify parsing a simple config file
 *****************************************************************************/
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
          writer.write("\t\"Graph\": \"Nets/rosvall.net\",\n")
          writer.write("\t\"spark configs\": {\n")
            writer.write("\t\t\"Master\": \"local[*]\",\n")
            writer.write("\t\t\"num executors\": \"1\",\n")
            writer.write("\t\t\"executor cores\": \"8\",\n")
            writer.write("\t\t\"driver memory\": \"10G\",\n")
            writer.write("\t\t\"executor memory\": \"2G\"\n")
          writer.write("\t},\n")
          writer.write("\t\"Algorithm\": {\n")
            writer.write("\t\"Community detection algorithm\": \"InfoFlow\",\n")
            writer.write("\t\"PageRank tele\": 0.15,\n")
            writer.write("\t\"PageRank error threshold factor\": 20\n")
          writer.write("\t},\n")
          writer.write("\t\"log\": {\n")
            writer.write("\t\t\"log path\": \"Output/log.txt\",\n")
            writer.write("\t\t\"Parquet path\": \"\",\n")
            writer.write("\t\t\"RDD path\": \"\",\n")
            writer.write("\t\t\"txt path\": \"Output/vertex.txt\",\n")
            writer.write("\t\t\"Full Json path\": \"\",\n")
            writer.write("\t\t\"Reduced Json path\": \"Output/graph.json\",\n")
            writer.write("\t\t\"debug\": \"true\"\n")
          writer.write("\t}\n")
        writer.write("}\n")
      writer.close

      // verify parsing
      val configFile = ConfigFile(filename)
      assert( configFile.graphFile === "Nets/rosvall.net" )

      assert( configFile.sparkConfigs.master === "local[*]" )
      assert( configFile.sparkConfigs.numExecutors === "1" )
      assert( configFile.sparkConfigs.executorCores === "8" )
      assert( configFile.sparkConfigs.driverMemory === "10G" )
      assert( configFile.sparkConfigs.executorMemory === "2G" )

      assert( configFile.algoParams.algoName === "InfoFlow" )
      assert( configFile.algoParams.tele === 0.15 )

      assert( configFile.logFile.pathLog === "Output/log.txt" )
      assert( configFile.logFile.pathParquet === "" )
      assert( configFile.logFile.pathRDD === "" )
      assert( configFile.logFile.pathTxt === "Output/vertex.txt" )
      assert( configFile.logFile.pathFullJson === "" )
      assert( configFile.logFile.pathReducedJson === "Output/graph.json" )
      assert( configFile.logFile.debug === true )
    }
    finally {
      val file = new File(filename)
      file.delete
    }
  }
}
