import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object InfoFlowMain {
  def main( args: Array[String] ): Unit = {

  /***************************************************************************
   * Main function
   ***************************************************************************/

  /***************************************************************************
   * read in from args
   ***************************************************************************/
    if( args.size < 4 ) {
      println("InfoFlow: requires 4 arguments:")
      println("[pajek.net] [merge.algorithm] [output.dir] [dampingFactor]")
      return
    }
    val pajekFile: String = args(0)
    val logFile = new LogFile( args(2), false )
    val mergeAlgo: MergeAlgo =
      if( args(1) == "InfoMap" ) new InfoMap
      else if( args(1) == "InfoFlow" ) new InfoFlow
      else throw new Exception("Merge algorithm must be InfoMap or InfoFlow")
    val dampingFactor: Double = args(3).toDouble

  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
    val conf = new SparkConf()
      .setAppName("InfoMap TwoLevel Test")
      //.setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")

  /***************************************************************************
   * read pajek file and solve
   ***************************************************************************/
    val pajek = new PajekFile(sc,pajekFile)
    val nodes = new Nodes(pajek,dampingFactor,1e-3)
    val initPartition = Partition.init(nodes)
    val finalPartition = mergeAlgo(initPartition,logFile)

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
  logFile.save( finalPartition.partitioning, "partition", false )

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
    sc.stop
  }
}
