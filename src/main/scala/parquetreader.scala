import org.apache.spark.sql._
import org.apcahe.spark.rdd.RDD

object ParquetReader
{
  def apply( sc: SparkContext, filename: String ): Graph = {
    val jsonReader = new JsonReader(filename)
    val verticesFile = jsonReader.getVal("Vertex File").toString
    val edgesFile = jsonReader.getVal("Edge File").toString

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val vertices = sqlContext.read.parquet(verticesFile)
    val edges = sqlContext.read.parquet(edgesFile)
    Graph(vertices,edges)
  }
}
