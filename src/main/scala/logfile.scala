/*((**************************************************************************
 * Helper class to write log merging progress and save graph data
 *
 * Data:
 * merging progress data is dictated by the specific merge algorithm
 * generally involving the code length, number of merges, number of modules;
 * network data involve:
 *   vertices: | id , size (number of nodes) , prob (ergodic frequency) |
 *   edges: | src , dst , weight (exit prob w/o tele) |
 * equally important is the original graph
 * stored in Graph case class
 * which provides a mapping from each node to each module index
 * and the names of each node
 * to be used in conjunction with the partitioning data
 *     vertices: | id , name , module |
 *     edges:    | from , to , exit prob. w/o tele |
 *
 * File formats:
 * merging progress data is written to a plain text file
 * graph data saving format(s) is specified in constructor
 * options include plain text file, Parquet, Json
 * partitioning data is saved in the same format as graph data
 *
 * Debugging data:
 * each LogFile instantiation may or may not be for debugging purpose
 * each operation might or might not be for debugging purpose
 * so that an operation is only performed if:
 *   (1) the operation is not for debugging, OR
 *   (2) the log file object is for debugging
 *((**************************************************************************/

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.graphframes._

import java.io._

sealed class LogFile(
  /***************************************************************************
   * path names, INCLUDING file names and extensions
   * if caller does not want to save in format, provide empty path
   ***************************************************************************/
  val pathLog:       String,   // plain text file path for merge progress data
  val pathParquet:   String,   // parquet file path for graph data
  val pathRDD:       String,   // RDD text file path for graph data
  val pathJson:      String,   // local Json file path for graph data

  /***************************************************************************
   * flags to specify whether associated graph data are saved
   ***************************************************************************/
  val savePartition: Boolean,  // whether to save partitioning data
  val saveName:      Boolean,  // whether to save node naming data

  /***************************************************************************
   * a logging operation is only performed if:
   *   (1) the operation is not for debugging, OR
   *   (2) the log file object is for debugging
   ***************************************************************************/
  val debug:         Boolean   // whether to print debug details
)
{

  /***************************************************************************
   * log file construction, writing, and closing
   ***************************************************************************/

  // create file to store the loop of code lengths
  val logFile = if( !pathLog.isEmpty ) {
    val file = new File( pathLog )
    new PrintWriter(file)
  }
  else null

  def write( msg: String, debugging: Boolean )
    = if( !pathLog.isEmpty && ( !debugging || debug ) ) {
      logFile.append(msg)
      logFile.flush
    }
  def close = if( !pathLog.isEmpty ) logFile.close

  /***************************************************************************
   * save graph into formats specified from object parameters
   ***************************************************************************/
  def save(
    // network: reduced graph, where each node is a community
    network: Network,
    // graphFile: original graph, all nodes and edges
    // vertices: | id , name , module |
    // edges: | from , to , exit prob. w/o tele |
    graphFile: Graph,
    debugging: Boolean,
    debugExt: String // this string is appended to file name (for debugging)
  ): Unit = {

  /***************************************************************************
   * when debugging, an additional string is appended
   * after the file name and before the final dot
   * this helper function returns the full path before and after the dot
   ***************************************************************************/
    def filePathInsert( filePath: String, insertion: String ): String = {
      val regex = """(.*)\.(\w+)""".r
      filePath match {
        case regex(path,ext) => path +insertion +"."+ext
        case _ => filePath +insertion
      }
    }

  /***************************************************************************
   * a logging operation is only performed if:
   *   (1) the operation is not for debugging, OR
   *   (2) the log file object is for debugging
   ***************************************************************************/
    /*if( !debugging || debug ) {

  /***************************************************************************
   * saving to Parquet and RDD text routines are virtually identical
   * except they have different guards and call different functions
   * therefore, implement here two functions
   ***************************************************************************/
      // routine to save all vertices, edges, partitioning and naming
      def saveDF(
        guard: Boolean, savePartition: Boolean, saveName: Boolean,
        debugging: Boolean, filePath: String, debugExt: String
      ): Unit = {
        // routine to save an individual dataframe
        def saveStruct( guard: Boolean, debugging: Boolean,
          filePath: String, fileExt: String, debugExt: String,
          saveFn: ( (String,DataFrame) => Unit ), struct: DataFrame
        ): Unit = {
          if( guard ) {
            val filename = if( !debugging )
              filePathInsert( filePath, fileExt )
            else
              filePathInsert( filePath, debugExt+fileExt )
            saveFn( filename, struct )
          }
        }

        if( guard ) {
          saveStruct( true,
            debugging, pathParquet, "-vertices", debugExt,
            LogFile.saveParquet, network.vertices )
          saveStruct( true,
            debugging, pathParquet, "-edges", debugExt,
            LogFile.saveParquet, network.edges )
          saveStruct( savePartition,
            debugging, pathParquet, "-partition", debugExt,
            LogFile.saveParquet, partition )
          saveStruct( saveName,
            debugging, pathParquet, "-name", debugExt,
            LogFile.saveParquet, network.name )
        }
      }

      saveDF( !pathParquet.isEmpty, savePartition, saveName,
        debugging, pathParquet, debugExt )
      saveDF( !pathRDD.isEmpty, savePartition, saveName,
        debugging, pathRDD, debugExt )

      if( !pathRDD.isEmpty ) {
        saveStruct( true, pathRDD, "-vertices",
          LogFile.saveRDD, network.vertices )
        saveStruct( true, pathRDD, "-edges",
          LogFile.saveRDD, network.edges )
        saveStruct( savePartition, pathRDD, "-partition",
          LogFile.saveRDD, network.partition )
        saveStruct( saveName, pathRDD, "-name",
          LogFile.saveRDD, network.name )
      }

  /***************************************************************************
   * routine to save Json
   ***************************************************************************/

      /*if( !pathJson.isEmpty ) {
        LogFile.saveJson( pathJson, graph ) /////////////// THIS NEEDS A LOT MORE WORK!!!
      }*/
    }*/
    {} // use this line to return unit for now
  }
}

object LogFile
{
  def saveParquet( filename: String, struct: DataFrame ): Unit
  = struct.write.parquet(filename) // check syntax
  def saveRDD( filename: String, struct: DataFrame ): Unit
  = struct.rdd.saveAsTextFile(filename)

  def saveJson( filename: String, graph: GraphFrame ): Unit = ???
}
