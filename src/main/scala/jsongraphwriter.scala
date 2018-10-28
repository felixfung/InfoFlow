sealed case class JsonGraph
{
  // | id , name , size , freq |
  vertices: Array[(Long,(String,Long,Double))],
  // |from id , to id , weight |
  edges: Arrau[((Long,Long),Double)]
}

sealed object JsonGraphWriter
{
  def apply( filename: String, graph: JsonGraph ): Unit = {
  }
}
