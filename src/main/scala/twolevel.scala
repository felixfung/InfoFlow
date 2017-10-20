import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object TwoLevelLocal {
  def main( args: Array[String] ): Unit = {

  /***************************************************************************
   * Main function
   ***************************************************************************/

  /***************************************************************************
   * read in from args
   ***************************************************************************/
    if( args.size < 4 ) {
      println("InfoMap: requires 4 arguments. Usage:")
      println("./InfoMap pajek.net merge.algorithm output.dir dampingFactor")
      return
    }
    val pajekFile: String = args(0)
    val outputDir = args(2)
    val mergeAlgo: MergeAlgo =
      if( args(1) == "InfoMap" ) new InfoMap(outputDir)
      else if( args(1) == "InfoFlow" ) new InfoFlow(outputDir)
      else throw new Exception("Merge algorithm must be InfoMap or InfoFlow")
    val dampingFactor: Double = args(3).toDouble

  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
    val conf = new SparkConf()
      .setAppName("InfoMap TwoLevel Test")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

  /***************************************************************************
   * read pajek file and solve
   ***************************************************************************/
    val pajek = new PajekFile(sc,pajekFile)
    val nodes = new Nodes(pajek,dampingFactor,1e-3)
    val initPartition = Partition.init(nodes)
    val finalPartition = mergeAlgo(initPartition)

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
    sc.stop
  }
}
