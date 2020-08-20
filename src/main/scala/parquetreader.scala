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
  def apply( sc: SparkContext, filename: String, logFile: LogFile ): Graph = {
    val jsonReader = new JsonReader(filename)
    val verticesFile = jsonReader.getObj("Vertex File").value.toString
    val edgesFile = jsonReader.getObj("Edge File").value.toString

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val vertices = sqlContext.read.parquet(verticesFile)
    .rdd
    .map {
      case Row( idx: Long, name: String, module: Long )
      => (idx,(name,module))
    }
	logFile.write(s"Read in vertex information from $verticesFile\n",false)

    val edges = sqlContext.read.parquet(edgesFile)
    .rdd
    .map {
      case Row( from: Long, to: Long, weight: Double )
      => (from,(to,weight))
    }
	logFile.write(s"Read in edge information from $edgesFile\n",false)

    Graph( vertices, edges )
  }
}
