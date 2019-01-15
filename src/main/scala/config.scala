/*****************************************************************************
 * class to read in config file
 * class that reads in a Json config file
 * usage: val configFile = new ConfigFile("config.json")
 *        val master = configFile.master
 *        and so on to access other properties
 *****************************************************************************/

import scala.util.parsing.json._

sealed case class ConfigFile
(
  graphFile: String,
  sparkConfigs: ConfigFile.SparkConfigs,
  algoParams: ConfigFile.AlgoParams,
  logFile: ConfigFile.LogParams
)

object ConfigFile
{
  // class that holds Spark configurations
  sealed case class SparkConfigs(
    val master:         String,
    val numExecutors:   String,
    val executorCores:  String,
    val driverMemory:   String,
    val executorMemory: String
  )

  // class that holds algorithmic parameters
  sealed case class AlgoParams(
    val algoName: String,
    val tele: Double,
    val errThFactor: Double
  )

  // class that holds parameters for log file
  // used in ConfigFile class
  sealed case class LogParams(
    val pathLog:          String, // plain text log file path
    val pathParquet:      String, // parquet file path for graph data
    val pathRDD:          String, // RDD text file path for graph data
    val pathTxt:          String, // local text file path for graph vertex data
    val pathFullJson:     String, // local Json file path for graph data
    val pathReducedJson:  String, // local Json file path for graph data
    val debug:            Boolean // whether to print debug details
  )

  def apply( filename: String ): ConfigFile = {
    val rawJson = new JsonReader(filename)
    ConfigFile(
      rawJson.getVal("Graph").toString,
      ConfigFile.SparkConfigs(
        rawJson.getVal("spark configs","Master").toString,
        rawJson.getVal("spark configs","num executors").toString,
        rawJson.getVal("spark configs","executor cores").toString,
        rawJson.getVal("spark configs","driver memory").toString,
        rawJson.getVal("spark configs","executor memory").toString
      ),
      ConfigFile.AlgoParams(
        rawJson.getVal("Algorithm","Community detection algorithm").toString,
        rawJson.getVal("Algorithm","PageRank tele").toString.toDouble,
        rawJson.getVal("Algorithm","PageRank error threshold factor")
          .toString.toDouble
      ),
      ConfigFile.LogParams(
        rawJson.getVal("log","log path").toString,
        rawJson.getVal("log","Parquet path").toString,
        rawJson.getVal("log","RDD path").toString,
        rawJson.getVal("log","txt path").toString,
        rawJson.getVal("log","Full Json path").toString,
        rawJson.getVal("log","Reduced Json path").toString,
        rawJson.getVal("log","debug").toString.toBoolean
      )
    )
  }
}
