import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object InfoFlowMain {
  /***************************************************************************
   * Main function
   ***************************************************************************/
  def main( args: Array[String] ): Unit = {

  /***************************************************************************
   * read in config file
   ***************************************************************************/

    // check argument size
    if( args.size > 1 ) {
      println("InfoFlow: requires 0-1 arguments:")
      println("./InfoFlow [alternative config file]")
      return
    }

    // use default or alternative config file name
    val configFileName =
      if( args.size == 0 ) "config.json"
      else /*args.size==1*/ args(0)
    val config = new Config(configFileName)

    // initialize parameters from config file
    val pajekFile = config.pajekFile
    val dampingFactor = config.dampingFactor
    val mergeAlgo: MergeAlgo =
      if( config.mergeAlgo == "InfoMap" ) new InfoMap
      else if( config.mergeAlgo == "InfoFlow" ) new InfoFlow
      else throw new Exception("Merge algorithm must be InfoMap or InfoFlow")
    val logFile = new LogFile(
      config.logDir,
      config.logWriteLog, config.rddText,
      config.rddJSon, config.logSteps
    )

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
   * Output
   ***************************************************************************/
    if( !logFile.logSteps )
      logFile.saveJSon( finalPartition, "graph.json", false )

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
    sc.stop
  }
}
