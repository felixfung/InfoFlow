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

    logFile.write(s"Reading ${config.graphFile}...",false)
    val graph0: Graph = GraphReader( sc, config.graphFile )
    logFile.write(" Done\n",false)

    logFile.write("Initializing partitioning, calculating PageRank...",false)
    val part0: Partition = Partition.init( graph0, config.tele )
    logFile.write(" Done\n",false)

    logFile.write(s"Using ${config.algorithm} algorithm:\n",false)
    val (graph1,part1) = communityDetection( graph0, part0, logFile )

    logFile.write("Save final graph...",false)
    logFile.save( graph1, part1, false, "" )
    logFile.write(" Done\n",false)

    logFile.close

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
    sc.stop
  }
}
