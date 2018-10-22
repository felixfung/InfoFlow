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
    val config = ConfigFile(configFileName)

    // initialize community detection algorithm
    val communityDetection = CommunityDetection.choose( config.algorithm )

    // create log file object
    val logFile = new LogFile(
      config.logFile.pathLog,
      config.logFile.pathParquet,
      config.logFile.pathRDD,
      config.logFile.pathJson,
      config.logFile.savePartition,
      config.logFile.saveName,
      config.logFile.debug
    )

  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
    val conf = new SparkConf
      .setAppName("InfoFlow")
      .setMaster( config.master )
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")

  /***************************************************************************
   * read file, solve, save
   ***************************************************************************/
    val graph: Graph = GraphReader( sc, config.graphFile )
    val net0: Network = Network.init( graph, config.tele )
    val net1: Network = communityDetection( graph, net0, logFile )
    logFile.save( net1, graph, false, "" )

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
    sc.stop
  }
}
