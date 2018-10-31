/***************************************************************************
 * Write graph into local Json file for visualization
 ***************************************************************************/

import java.io._

sealed case class JsonGraph
(
  // | id , name , module , size |
  vertices: Array[(Long,(String,Long,Double))],
  // |from id , to id , weight |
  edges: Array[((Long,Long),Double)]
)

object JsonGraphWriter
{
  def apply( filename: String, graph: JsonGraph ): Unit = {

    // open file
    val file = new PrintWriter( new File(filename) )

    // write node data
    if( !graph.vertices.isEmpty ) {
      file.write( "{\n\t\"nodes\": [\n" )
      val nodeCount = graph.vertices.size
      for( idx <- 0 to nodeCount-1 ) {
        graph.vertices(idx) match {
          case (id,(name,module,size)) => {
            file.write(
              "\t\t{\"id\": \"" +s"$id" +"\", "
             +"\"size\": \"" +s"$size" +"\", "
             +"\"name\": \"" +s"$name" +"\", "
             +"\"group\": \"" +s"$module" +"\""
             +"}"
            )
            if( idx < nodeCount-1 )
              file.write(",")
            file.write("\n")
          }
        }
      }
      file.write( "\t]" )
    }

    // write edge data
    if( !graph.edges.isEmpty ) {
      file.write( ",\n\t\"links\": [\n" )
      val edgeCount = graph.edges.size
      for( idx <- 0 to edgeCount-1 ) {
        graph.edges(idx) match {
          case ((from,to),weight) =>
            file.write(
              "\t\t{\"source\": \"" +s"$from" +"\", "
             +"\"target\": \"" +s"$to" +"\", "
             +"\"value\": \"" +s"$weight" +"\""
             +"}"
            )
            if( idx < edgeCount-1 )
              file.write(",")
            file.write("\n")
        }
      }
      file.write("\t]")
    }

    // close file
    file.write( "\n}" )
    file.close
  }

  /***************************************************************************
   * this functions saves a graph with normalized vertex radius
   * scale the vertex size linearly,
   * so the smallest vertex has size 1
   * and the biggest has size 4
   ***************************************************************************/
  def writeNormal( filename: String, graph: JsonGraph ) {
    val vertices = {
      val min = graph.vertices.map {
        case (_,(_,_,p)) => p
      }
      .reduce( (a,b) => if( a<=b ) a else b )
      val max = graph.vertices.map {
        case (_,(_,_,p)) => p
      }
      .reduce( (a,b) => if( a<=b ) b else a )
      graph.vertices.map {
        case (idx,(name,id,p)) => (idx,(name,id, 1 +(p-min)*3/(max-min) ))
      }
    }
    JsonGraphWriter( filename, JsonGraph( vertices, graph.edges ) )
  }
}
