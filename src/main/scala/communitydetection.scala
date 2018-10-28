import org.apache.spark.rdd.RDD
import java.lang.Math

abstract class CommunityDetection {
  def apply( graph: Graph, net: Network, logFile: LogFile ): ( Graph, Network )
}

object CommunityDetection {
  def choose( algorithm: String ): CommunityDetection = {
    if( algorithm == "InfoMap" ) new InfoMap
    else if( algorithm == "InfoFlow" ) new InfoFlow
    else throw new Exception(
      "Community detection algorithm must be InfoMap or InfoFlow"
    )
  }

  def calCodelength(
    vertices: RDD[(Long,(Long,Double,Double,Double))], probSum: Double
  ): Double = {
    if( vertices.count == 1 ) {
      // if the entire graph is merged into one module,
      // there is easy calculation
      -probSum
    }
    else {
      val qi_sum = vertices.map {
        case (_,(_,_,_,q)) => q
      }
      .sum
      val otherSum = vertices.map {
        case (_,(_,p,_,q)) => -2*plogp(q) +plogp(p+q)
      }
      .sum
      otherSum +plogp(qi_sum) -probSum
    }
  }

  def calQ( nodeNumber: Long, n: Long, p: Double, tele: Double, w: Double ) =
    tele*(nodeNumber-n)/(nodeNumber-1)*p +(1-tele)*w

  def calDeltaL(
    network: Network,
    n1: Long, n2: Long, p1: Double, p2: Double,
    w12: Double,
    qi_sum: Double, q1: Double, q2: Double
  ) = {
    val q12 = calQ( network.nodeNumber, n1+n2, p1+p2, network.tele, w12 )
    if( q12 > 0 && qi_sum+q12-q1-q2>0 ) (
      +plogp( qi_sum +q12-q1-q2 ) -plogp(qi_sum)
      -2*plogp(q12) +2*plogp(q1) +2*plogp(q2)
      +plogp(p1+p2+q12) -plogp(p1+q1) -plogp(p2+q2)
    )
    else {
      //throw new Exception("caught some crap: one giant module")
      -network.probSum -network.codelength
    }
  }

  /***************************************************************************
   * math function of plogp(x) for calculation of code length
   ***************************************************************************/
  def log( double: Double ) = Math.log(double)/Math.log(2.0)
  def plogp( double: Double ) = double*log(double)
}
