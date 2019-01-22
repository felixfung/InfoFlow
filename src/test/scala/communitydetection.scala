/***************************************************************************
 * function to perform a functional test on community detection
 * community detection algorithm can be InfoFlow or InfoMap
 ***************************************************************************/

import org.apache.spark.SparkContext

object CommunityDetectionTest
{
  def apply(
    sc: SparkContext,
    communityDetection: CommunityDetection,
    pajekFile: String
  ): ( Double, Array[(Long,Long)] ) = {
    val logFile = new LogFile(sc,"","","","","","",false)
    val graph0 = PajekReader( sc, pajekFile, logFile )
    val part0 = Partition.init( graph0, 0.15, 20, logFile )
    val (graph1,part1) = communityDetection( graph0, part0, logFile )
    val codelength = part1.codelength
    val partition = graph1.vertices.collect.sorted.map {
      case (idx,(name,module)) => (idx,module)
    }
    ( codelength, partition )
  }
}
