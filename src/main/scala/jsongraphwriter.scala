/***************************************************************************
 * Write graph into local Json file for visualization
 ***************************************************************************/

import java.io._

sealed case class JsonGraph
(
  // | id , name , module , couunt, size |
  vertices: Array[(Long,(String,Long,Long,Double))],
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
          case (id,(name,module,count,size)) => {
            file.write(
              "\t\t{\"id\": \"" +s"$id" +"\", "
             +"\"size\": \"" +s"$size" +"\", "
             +"\"count\": \"" +s"$count" +"\", "
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
}
