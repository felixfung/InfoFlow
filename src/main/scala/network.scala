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
  nodeNumber: Long, tele: Double,
  // | idx , n , p , w , q |
  vertices: RDD[(Long,(Long,Double,Double,Double))],
  // | index from , index to , weight |
  edges: RDD[(Long,(Long,Double))],
  probSum: Double, // sum of ergodic frequency, for codelength calculation
  codelength: Double // codelength given modules
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
    val ergodicFreq = PageRank( graph, tele )
    ergodicFreq.cache

    // modular information
    // since transition probability is normalized per "from" node,
    // w and q are mathematically identical to p
    // as long as there is at least one connection
    // | id , size , prob , exitw , exitq |
    val vertices: RDD[(Long,(Long,Double,Double,Double))] = {

      val exitw: RDD[(Long,Double)] = graph.edges
      .join( ergodicFreq )
      .map {
        case (from,((to,weight),ergodicFreq)) => (from,ergodicFreq*weight)
      }
      .reduceByKey(_+_)

      ergodicFreq.leftOuterJoin(exitw)
      .map {
        case (idx,(freq,Some(w))) => (idx,(1,freq,w,tele*freq+(1-tele)*w))
        case (idx,(freq,None)) => (idx,(1,freq,0,tele*freq))
      }
    }

    val probSum = ergodicFreq.map {
      case (_,p) => p
    }
    .sum

    val codelength = CommunityDetection.calCodelength( vertices, probSum )

    // return Network object
    Network(
      nodeNumber, tele,
      vertices, graph.edges,
      probSum, codelength
    )
  }
}
