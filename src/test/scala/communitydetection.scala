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
    val infoFlow = new InfoFlow
    val graph0 = PajekReader(sc,"Nets/trivial.net")
    val net0 = Network.init(graph0,0.85)
    val logFile = new LogFile("","","","",false,false,false)
    val (graph1,net1) = infoFlow( graph0, net0, logFile )
    val codelength = net1.codelength
    val partition = graph1.vertices.collect.sorted.map {
      case (idx,(name,module)) => (idx,module)
    }
    println(net0.codelength)
    println(net1.codelength)
    partition.foreach(println)
    ( codelength, partition )
  }
}
