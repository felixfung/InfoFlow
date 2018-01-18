abstract class MergeAlgo {
  def apply( partition: Partition, logFile: LogFile ): Partition
}

class InfoMix extends MergeAlgo
{
  def apply( initPartition: Partition, logFile: LogFile ): Partition = {
    val infoFlow = new InfoFlow
    val infoMap  = new InfoMap

    // use the same log file, but append rather than overwrite
    val appendLog = new LogFile(
      logFile.outputDir,
      logFile.writeLog,
      logFile.rddText,
      logFile.rddJSon,
      logFile.logSteps,
      true
    )

    val interPartition = infoFlow(initPartition,logFile)
    appendLog.write("\n")
    infoMap(interPartition,appendLog)
  }
}

object MergeAlgo {
  def choose( algo: String ): MergeAlgo =
    if( algo == "InfoMap" )
      new InfoMap
    else if( algo == "InfoFlow" )
      new InfoFlow
    else if( algo == "InfoFlowMap" )
      new InfoMix
    else
      throw new Exception( "Merge algorithm must be:"
        +"InfoMap, InfoFlow, or InfoFlowMap" )
}
