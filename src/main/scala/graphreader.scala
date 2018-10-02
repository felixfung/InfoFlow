object GraphReader
{
  def apply(  sc: SparkContext, val filename: String ): Graph = {
    val regex = """(.*)\.(\w+)""".r
    filename match {
      case regex(_,ext) => {
        if( ext.toLowerCase == "net" )
          PajekReader( sc, filename )
        else if( ext.toLowerCase == "ext" )
          ParquetReader( sc, filename )
        else
          throw new Exception(
            "File must be Pajek net file (.net) or Parquet file (.parquet)"
          )
      }
      case _ => throw new Exception("Graph file has no file extension")
    }
  }
}
