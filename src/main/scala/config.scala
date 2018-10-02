  /***************************************************************************
   * class to read in config file
   * class that reads in a Json config file
   * usage: val configFile = new ConfigFile("config.json")
   *        val master = configFile.master
   *        and so on to access other properties
   ***************************************************************************/

import scala.util.parsing.json._

sealed case class ConfigFile
(
  master: String,
  graphFile: String,
  algorithm: String,
  tele: Double,
  logFile: ConfigFile.LogParams
)

object ConfigFile
{
  // class that holds parameters for log file
  // used in ConfigFile class
  sealed case class LogParams(
    val pathLog:       String,   // plain text file path for merge progress data
    val pathParquet:   String,   // parquet file path for graph data
    val pathRDD:       String,   // RDD text file path for graph data
    val pathJson:      String,   // local Json file path for graph data
    val savePartition: Boolean,  // whether to save partitioning data
    val saveName:      Boolean,  // whether to save node naming data
    val debug:         Boolean   // whether to print debug details
  )

  def apply( filename: String ): ConfigFile = {
    val rawJson = new JsonReader(filename)
    ConfigFile(
      rawJson.getVal("Master").toString,
      rawJson.getVal("Graph").toString,
      rawJson.getVal("Algo").toString,
      rawJson.getVal("tele").toString.toDouble,
      ConfigFile.LogParams(
        rawJson.getVal("log","log path").toString,
        rawJson.getVal("log","Parquet path").toString,
        rawJson.getVal("log","RDD path").toString,
        rawJson.getVal("log","Json path").toString,
        rawJson.getVal("log","save partition").toString.toBoolean,
        rawJson.getVal("log","save name").toString.toBoolean,
        rawJson.getVal("log","debug").toString.toBoolean
      )
    )
  }
}
