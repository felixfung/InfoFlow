/*****************************************************************************
 * static function that delegates to PajekReader or ParquetReader
 * based on file extension
 *****************************************************************************/

import org.apache.spark.SparkContext

object GraphReader
{
  def apply( sc: SparkContext, filename: String ): Graph = {
    val regex = """(.*)\.(\w+)""".r
    val graph: Graph = filename match {
      case regex(_,ext) => {
        if( ext.toLowerCase == "net" )
          PajekReader( sc, filename )
        else if( ext.toLowerCase == "parquet" )
          ParquetReader( sc, filename )
        else
          throw new Exception(
            "File must be Pajek net file (.net) or Parquet file (.parquet)"
          )
      }
      case _ => throw new Exception("Graph file has no file extension")
    }
    graph.vertices.localCheckpoint
	graph.vertices.cache
    val force1 = graph.vertices.count
    graph.edges.localCheckpoint
	graph.edges.cache
    val force2 = graph.edges.count
    graph
  }
}
