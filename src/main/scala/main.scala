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

  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
    val conf = new SparkConf()
      .setAppName("InfoFlow")
      .setMaster( config.master )
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")

  /***************************************************************************
   * read file, solve, save
   ***************************************************************************/

    // create log file object
    val logFile = new LogFile(
      sc,
      config.logFile.pathLog,
      config.logFile.pathParquet,
      config.logFile.pathRDD,
      config.logFile.pathTxt,
      config.logFile.pathFullJson,
      config.logFile.pathReducedJson,
      config.logFile.debug
    )

    val graph0: Graph = GraphReader( sc, config.graphFile )
    val net0: Network = Network.init( graph0, config.tele )
    val (graph1,net1) = communityDetection( graph0, net0, logFile )
    logFile.save( graph1, net1, false, "" )
    logFile.close

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
    sc.stop
  }
}
