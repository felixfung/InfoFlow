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
              "\t\t{\"id\": \"" +id.toString +"\""
             +"\", \"size\": \"" +size.toString +"\""
             +"\", \"name\": \"" +name.toString +"\""
             +"\", \"group\": \"" +module.toString +"\""
             +"}"
            )
            if( idx < nodeCount-1 )
              file.write(",")
            file.write("\n")
          }
        }
      }
      file.write( "\t],\n" )
    }

    // write edge data
    if( !graph.edges.isEmpty ) {
      file.write( "\t\"links\": [\n" )
      val edgeCount = graph.edges.size
      for( idx <- 0 to edgeCount-1 ) {
        graph.edges(idx) match {
          case ((from,to),weight) =>
            file.write(
              "\t\t{\"source\": \"" +from.toString +"\""
             +"\", \"target\": \"" +to.toString +"\""
             +"\", \"width\": \"" +weight.toString +"\""
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
