/*****************************************************************************
 * store graph data that are relevant to community detection
 * which involves a few scalar (Double or Long) variables
 * and a graph (vertices and edges)
 * importantly, the graph stores reduced graph
 * where each node represents a module/community
 * this reduced graph can be combined with the original graph (Graph object)
 *****************************************************************************/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

sealed case class Network
(
  nodeNumber, tele,
  // | idx , n , p , w , q |
  vertexProp: RDD[(Long,(Long,Double,Double,Double))],
  // | index from , index to , weight |
  edgeWeight: RDD[(Long,(Long,Double))],
  probSum, // sum of ergodic frequency, needed for codelength calculation
  codelength // codelength given modules
)

/*****************************************************************************
 * given a Graph (probably from GraphFile.graph)
 * and the PageRank teleportation probability
 * calculate PageRank and exit probabilities for each node
 * these are put and returned to a Network object
 * which can be used for community detection
 *****************************************************************************/

object Network
{
  def init( graph: Graph, tele: Double ): Network = {

    val nodeNumber: Long = graph.vertices.count

    // exit probability from each vertex
    val ergodicFreq = calErgodicFreq( graph, tele )
    ergodicFreq.cache

    // modular information
    // since transition probability is normalized per "from" node,
    // w and q are mathematically identical to p
    // as long as there is at least one connection
    // | id , size , prob , exitw , exitq |
    val vertexProp: RDD[(Long,Long,Double,Double,Double)] = {

      val exitw: RDD[(Long,Double)] = graph.edges
      .join( ergodicFreq )
      .map {
        case (from,((to,weight),ergodicFreq)) => (from,ergodicFreq*weight)
      }
      .reduceByKey(_+_)

      ergodicFreq.leftOuterJoin(wi)
      .map {
        case (idx,freq,Some(w)) => (idx,1,freq,w,tele*freq+(1-tele)*w))
        case (idx,(freq,None)) => (idx,(1,freq,0,tele*freq))
      }
    }

    val probSum = ergodicFreq.map {
      case (_,p) => p
    }
    .sum

    val codelength = CommunityDetection.calCodelength( vertexProp, probSum )

    // return Network object
    Network(
      nodeNumber, tele,
      vertexProp, graph.edges,
      probSum, codelength
    )
  }

  private def calErgodicFreq( graph: Graph, tele: Double )
  : RDD[(Long,Double)] = {

    val edges: Matrix = {

      // sum of weight of outgoing links
      val outLinkTotalWeight: RDD[(Long,Double)] = {
        graph.edges.map {
          case (from,to,weight) => (from,weight)
        }
      .reduceByKey(_+_)
      }
      outLinkTotalWeight.cache

      // nodes without outbound links are dangling"
      val dangling: RDD[Long] = graph.vertices.leftOuterJoin(outLinkTotalWeight)
      .filter {
        case (_,(_,Some(_))) => false
        case (_,(_,None)) => true
      }
      .map {
        case (idx,_) => idx
      }

      // dangling nodes jump to uniform probability
      val constCol = dangling.map (
        x => ( x, 1.0/n.toDouble )
      )

      // normalize the edge weights
      val normMat = graph.edges.join(outLinkTotalWeight)
      .map {
        case (from,((to,weight),totalweight)) => (from,(to,weight/totalweight))
      }

      Matrix( normMat, constCol )
    }

    // start with uniform ergodic frequency
    val freqUniform = graph.vertices.map {
      case (idx,_,_) => ( idx, 1.0/n.toDouble )
    }

    PageRank( edges, freqUniform, n, 1.0-tele, errTh, 0 )
  }
}
