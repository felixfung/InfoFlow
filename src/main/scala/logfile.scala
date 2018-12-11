/*((**************************************************************************
 * Helper class to write log merging progress and save graph data
 *
 * Data:
 * merging progress data is dictated by the specific merge algorithm
 * generally involving the code length, number of merges, number of modules;
 * partition data involve:
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

import org.apache.spark.SparkContext
import org.apache.spark.sql._ // needed to save as Parquet format

import java.util.Calendar

import java.io._

sealed class LogFile(
  val sc:               SparkContext, // needed to save as Parquet
  /***************************************************************************
   * path names, INCLUDING file names and extensions
   * if caller does not want to save in format, provide empty path
   ***************************************************************************/
  val pathLog:          String, // plain text file path for merge progress data
  val pathParquet:      String, // parquet file path for graph data
  val pathRDD:          String, // RDD text file path for graph data
  val pathTxt:          String, // local text file path for graph vertex data
  val pathFullJson:     String, // local Json file path for graph data
  val pathReducedJson:  String, // local Json file path for graph data

  /***************************************************************************
   * a logging operation is only performed if:
   *   (1) the operation is not for debugging, OR
   *   (2) the log file object is for debugging
   ***************************************************************************/
  val debug:            Boolean // whether to print debug details
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
      logFile.append(s"${Calendar.getInstance().getTime}: "+msg)
      logFile.flush
    }
  def close = if( !pathLog.isEmpty ) logFile.close

  /***************************************************************************
   * save graph into formats specified from object parameters
   ***************************************************************************/
  def save(
    // graphFile: original graph, all nodes and edges
    // vertices: | id , name , module |
    // edges: | from , to , exit prob. w/o tele |
    graph: Graph,
    // part: reduced graph, where each node is a community
    part: Partition,
    debugging: Boolean,
    debugExt: String // this string is appended to file name (for debugging)
  ): Unit = {

  /***************************************************************************
   * when debugging, an additional string is appended
   * after the file name and before the final dot
   * this helper function returns the full path before and after the dot
   ***************************************************************************/
    def splitFilepath( filepath: String ): (String,String) = {
      val regex = """(.*)\.(\w+)""".r
      filepath match {
        case regex(path,ext) => ( path, "."+ext )
        case _ => ( filepath, "" )
      }
    }

  /***************************************************************************
   * a logging operation is only performed if:
   *   (1) the operation is not for debugging, OR
   *   (2) the log file object is for debugging
   ***************************************************************************/
    if( !debugging || debug ) {
      val exext = if(debugging) debugExt else ""
      if( !pathParquet.isEmpty ) {
        val (filename,ext) = splitFilepath(pathParquet)
        LogFile.saveParquet( filename, exext+ext, graph, sc )
      }
      if( !pathRDD.isEmpty ) {
        val (filename,ext) = splitFilepath(pathRDD)
        LogFile.saveParquet( filename, exext+ext, graph, sc )
      }
      if( !pathTxt.isEmpty ) {
        val (filename,ext) = splitFilepath(pathTxt)
        LogFile.saveTxt( filename, exext+ext, graph )
      }
      if( !pathFullJson.isEmpty ) {
        val (filename,ext) = splitFilepath(pathFullJson)
        LogFile.saveFullJson( filename, exext+ext, graph )
      }
      if( !pathReducedJson.isEmpty ) {
        val (filename,ext) = splitFilepath(pathReducedJson)
        LogFile.saveReducedJson( filename, exext+ext, part )
      }
    }
  }
}

object LogFile
{
  def saveParquet( filename: String, ext: String,
    graph: Graph, sc: SparkContext ): Unit = {
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    graph.vertices.toDF.write.parquet( s"$filename-vertices$ext" )
    graph.edges.toDF.write.parquet( s"$filename-edges$ext" )
  }
  def saveRDD( filename: String, ext: String, graph: Graph ): Unit = {
    graph.vertices.saveAsTextFile( s"$filename-vertices$ext" )
    graph.edges.saveAsTextFile( s"$filename-edges$ext" )
  }

  // save as local text file
  // only vertex data.are saved,
  // since edge data can be saved via Json graphs
  def saveTxt( filename: String, ext: String, graph: Graph ): Unit = {
    // for spacing consistency, needs to pad vertex names with spacing
    def pad( string: String, totalLength: Int ): String = {
      var padding = ""
      for( i <- string.length+1 to totalLength )
        padding += " "
      string +padding
    }
    val file = new File( s"$filename$ext" )
    val txtFile = new PrintWriter(file)

    val vertices = graph.vertices.collect.sorted
    val maxNameLength = Math.max( graph.vertices.reduce {
      case ( (idx1,(name1,mod1)), (idx2,(name2,mod2)) ) =>
        if( name1.length > name2.length )
          (idx1,(name1,mod1))
        else
          (idx2,(name2,mod2))
    }
    ._2._1.length, 4 )

    txtFile.write(s"Index   | ${pad("Name",maxNameLength)} | Module \n")

    for( vertex <- vertices ) {
      vertex match {
        case (idx,(name,module)) => txtFile.write(
          "%7d | %s | %7d\n".format( idx, pad(name,maxNameLength), module )
        )
      }
    }
    txtFile.close
  }

  /***************************************************************************
   * save graph as Json for visualization
   * all nodes are printed with each module associated
   * according to community detection
   * this printing function probably used for demo purpose
   ***************************************************************************/
  def saveFullJson( filename: String, ext: String, graph: Graph ) = {
    // fake nodes to preserve group ordering/coloring
    val fakeNodes = graph.vertices.map {
      case (idx,_) => (-idx,("",idx,0.0))
    }
    .collect
    val vertices = graph.vertices.map {
      case (id,(name,module)) => (id,(name,module,1.0))
    }
    .collect ++fakeNodes
    val edges = graph.edges.map {
      case (from,(to,weight)) => ((from,to),weight)
    }
    .collect.sorted
    val newGraph = JsonGraph( vertices.sorted, edges )
    JsonGraphWriter( s"$filename$ext", newGraph )
  }

  /***************************************************************************
   * save graph as Json for visualization
   * each node is a module
   * names are always empty string
   ***************************************************************************/
  def saveReducedJson( filename: String, ext: String, part: Partition ) = {
    val vertices = part.vertices.map {
      case (id,(_,p,_,_)) => (id,(id.toString,id,p))
    }
    // Json is local file
    .collect.sorted
    val edges = part.edges.map {
      case (from,(to,weight)) => ((from,to),weight)
    }
    .collect.sorted
    val graph = JsonGraph( vertices, edges )
    JsonGraphWriter( s"$filename$ext", graph )
  }
}
