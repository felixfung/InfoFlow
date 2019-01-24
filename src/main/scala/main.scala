/*****************************************************************************
 * Main function
 *****************************************************************************/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object InfoFlowMain {
  def main( args: Array[String] ): Unit = {

  /***************************************************************************
   * read in config file
   ***************************************************************************/
    if( args.size > 1 ) {
      println("InfoFlow: requires 0-1 arguments:")
      println("./InfoFlow [alternative config file]")
      return
    }

    // use default or alternative config file name
    val configFileName = if( args.size == 0 ) "config.json" else args(0)
    val config = ConfigFile(configFileName)

  /***************************************************************************
   * Initialize structures; function definitions defined below
   ***************************************************************************/
    val graphFile = config.getObj(["Graph"]).toString
    val( spark, sc ) = initSpark( config.getObj(["spark configs"]) )
	val pageRankConfig = config.getObj(["PageRank"])
    val algoName = config.getObj(["Community Detection","name"]).toString
    val communityDetection = initCD( algoName )
    val logFile = initLog( config.getObj(["log"]) )

  /***************************************************************************
   * read, solve, save
   ***************************************************************************/
    logEnvironment( sc, logFile )
    val graph0: Graph = readGraph( sc, graphFile, logFile )
    val part0: Partition = initPartition( graph0, pageRankConfig, logFile )
    val(graph1,part1) = communityDetection( graph0, part0, algoName, logFile )
    saveFinalGraph( graph1, part1 )
    terminate(sc)
}

/*****************************************************************************
 * Below functions are implementations of function calls above
 *****************************************************************************/
object InfoFlowMain {
  /***************************************************************************
   * Initialize community detection algorithm
   ***************************************************************************/
    def initCD( algoName: String ) = CommunityDetection.choose( algoName )

  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
    def initSpark( sparkConfig: JsonObj ): (SparkConf,SparkContext) = {
      val master = sparkConfig.getVal("Master").toString,
      val numExecutors = sparkConfig.getVal("num executors").toString,
      val executorCores = sparkConfig.getVal("executor cores").toString,
      val driverMemory = sparkConfig.getVal("driver memory").toString,
      val executorMemory = sparkConfig.getVal("executor memory").toString
      val spark = new SparkConf()
        .setAppName("InfoFlow")
        .setMaster( config.sparkConfigs.master )
        .set( "spark.executor.instances", numExecutors )
        .set( "spark.executor.cores", executorCores )
        .set( "spark.driver.memory", driverMemory )
        .set( "spark.executor.memory", executorMemory )
      val sc = new SparkContext(spark)
      sc.setLogLevel("OFF")
      ( spark, sc )
    }

  /***************************************************************************
   * create log file object
   ***************************************************************************/
    def initLog( logConfig: JsonObj, sc: SparkContext ): LogFile = {
      new LogFile(
        sc,
        logConfig.getObj("log path").toString,
        logConfig.getObj(""Parquet path").toString,
        logConfig.getObj(""RDD path").toString,
        logConfig.getObj(""txt path").toString,
        logConfig.getObj(""Full Json path").toString,
        logConfig.getObj(""Reduced Json path").toString,
        logConfig.getObj(""debug").toString.toBoolean
      )
    }

  /***************************************************************************
   * log app version, spark version
   ***************************************************************************/
    def logEnvironment( sc: SparkContext, logFile: LogFile ): Unit = {
      val jar = sc.jars.head.split('/').last
      val version = jar.split('-').last.split('.').dropRight(1).mkString(".")
      logFile.write(s"Running ${sc.appName}, version: $version\n",false)
	  val jvmHeapSpace = Runtime.getRuntime().maxMemory/1024/1024
      logFile.write(
        s"Driver memory/Java heap size: $jvmHeapSpace Mb\n",
      false)
      logFile.write(s"Spark version: ${sc.version}\n",false)
      logFile.write(s"Spark configurations:\n",false)
	  spark.getAll.foreach{ case (x,y) => logFile.write(s"$x: $y\n",false) }
    }

  /***************************************************************************
   * read in graph
   ***************************************************************************/
    def readGraph( sc: SparkContext, graphFile: String,
    logFile: LogFile ): Graph = {
      logFile.write(s"Reading $graphFile\n",false)
      val graph = GraphReader( sc, graphFile, logFile )
      val vertices = graph.vertices.count
      val edges = graph.edges.count
      logFile.write(
        s"Read in network with $vertices nodes and $edges edges\n",
      false)
	  graph
    }

  /***************************************************************************
   * initialize partitioning
   ***************************************************************************/
    def initPartition( graph: Graph,
    pageRankConfig: JsonObj, logFile: LogFile ): Unit = {
      logFile.write(s"Initializing partitioning\n",false)
      val part = Partition.init( graph0, pageRankConfig, logFile )
      logFile.write(s"Finished initialization calculations\n",false)
      part
    }

  /***************************************************************************
   * perform community detection
   ***************************************************************************/
    def communityDetection( graph: Graph, part: Partition,
    algoName: String, logFile: LogFile ): (Graph,Partition) = {
      val algoName = config.getObj(["Community Detection","name"])
      logFile.write(s"Using $algoName algorithm:\n",false)
      communityDetection( graph0, part0, logFile )
    }

  /***************************************************************************
   * save final graph
   ***************************************************************************/
    def saveFinalGraph( graph: Graph, part: Partition ): Unit = {
      logFile.write(s"Save final graph\n",false)
      logFile.write(s"with ${part.vertices.count} modules"
        +s" and ${part.edges.count} connections\n",
      false)
      logFile.save( graph, part, false, "" )
    }

  /***************************************************************************
   * terminate program
   ***************************************************************************/
    def terminate( sc: SparkContext ): Unit = {
      logFile.write("InfoFlow Terminate\n",false)
      logFile.close
      sc.stop
    }
}