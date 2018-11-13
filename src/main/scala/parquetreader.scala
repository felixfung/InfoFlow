/*****************************************************************************
 * file to read parquet graph files
 * the first file to read is a Json file
 * which stores the vertex file and edge file,
 * where each of the latter is a parquet file
 *****************************************************************************/

import org.apache.spark.SparkContext

import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext

object ParquetReader
{
  def apply( sc: SparkContext, filename: String ): Graph = {
    val jsonReader = new JsonReader(filename)
    val verticesFile = jsonReader.getVal("Vertex File").toString
    val edgesFile = jsonReader.getVal("Edge File").toString

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val vertices = sqlContext.read.parquet(verticesFile)
    .rdd
    .map {
      case Row( idx: Long, name: String, module: Long )
      => (idx,(name,module))
    }
    val edges = sqlContext.read.parquet(edgesFile)
    .rdd
    .map {
      case Row( from: Long, to: Long, weight: Double )
      => (from,(to,weight))
    }
    Graph( vertices, edges )
  }
}
