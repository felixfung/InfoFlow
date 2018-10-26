/*****************************************************************************
 * ORIGINAL INFOMAP ALGORITHM
 * the function to partition of nodes into modules based on
 * greedily merging the pair of modules that gives
 * the greatest code length reduction
 * until code length is minimized
 *****************************************************************************/

import org.apache.spark.rdd.RDD

class InfoMap extends CommunityDetection
{

  def apply( graph: Graph, network: Network, logFile: LogFile )
  : ( Graph, Network ) = {
    @scala.annotation.tailrec
    def recursiveMerge(
      loop: Int,
      qi_sum: Double,
      graph: Graph,
      network: Network,
      mergeList: RDD[((Long,Long),InfoMap.Merge)]
    ): ( Graph, Network ) = {

      InfoMap.trim( loop, graph, network, mergeList )

      logFile.write(s"State $loop: code length ${network.codelength}\n",false)
      logFile.save( graph, network, true, "net"+loop.toString )

      if( mergeList.count == 0 )
        return InfoMap.terminate( loop, logFile, graph, network )

      val merge = InfoMap.findMerge(mergeList)
      if( merge._2.dL > 0 )
        return InfoMap.terminate( loop, logFile, graph, network )

      logFile.write(
        s"Merge $loop: merging modules ${merge._1._1}and ${merge._1._2}"
        +s" with code length reduction ${merge._2.dL}\n",
      false )

      val newGraph = InfoMap.calNewGraph( graph, merge )
      val newMergeList = InfoMap.updateMergeList(
        merge, mergeList, network,qi_sum )
      val newNetwork = InfoMap.calNewNetwork( network, merge )
      val new_qi_sum = InfoMap.cal_qi_sum( network, merge, qi_sum )

      recursiveMerge( loop+1, new_qi_sum, newGraph, newNetwork, newMergeList )
    }

    // invoke recursive merging calls
    val qi_sum = network.vertices.map {
      case (_,(_,_,_,q)) => q
    }.sum
    val edgeList = InfoMap.genMergeList( network, qi_sum )
    recursiveMerge( 0, qi_sum, graph, network, edgeList )
  }
}

/***************************************************************************
 * below are calculation functions
 ***************************************************************************/
object InfoMap
{
  /***************************************************************************
   * case class to store all associated quantities of a potential merge
   * format of each entry is
   *   (n1,n2,p1,p2,w1,w2,w1221,q1,q2,DeltaL12)
   *
   * a merge is symmetrical/commutative,
   * ie when two modules merge, it doesn't matter if a merge b or b merge a
   * hence, a Merge instance is an undirected edge
   * index1 should be made sure to be smaller than index2
   *
   * an RDD[Edge] forms a table
   * with this table, the modular properties are stored redundantly
   * but means that no table joining is required in the loop
   *
   * the InfoMap algorithm would involve:
   * first generating an RDD[Merge]
   * then updating it recursively
   ***************************************************************************/
  case class Merge
  (
    n1: Long, n2: Long, p1: Double, p2: Double,
    w1: Double, w2: Double, w1221: Double,
    q1: Double, q2: Double, dL: Double
  )
  /***************************************************************************
   * grab all edges and aggregate into nondirectional edges
   * store all associated modular properties
   * and calculate deltaL
   ***************************************************************************/
  def genMergeList( network: Network, qi_sum: Double ) = {
    // merges are  nondirectional edges
    network.edges.map {
      case (from,(to,weight)) =>
        if( from < to ) ((from,to),weight)
        else ((to,from),weight)
    }
    // via aggregation
    .reduceByKey(_+_)
    // now get associated vertex properties
    .map {
      case ((m1,m2),w1221) => (m1,(m2,w1221))
    }
    .join( network.vertices ).map {
      case (m1,((m2,w1221),(n1,p1,w1,q1)))
      => (m2,(m1,n1,p1,w1,q1,w1221))
    }
    .join( network.vertices ).map {
      case (m2,((m1,n1,p1,w1,q1,w1221),(n2,p2,w2,q2))) =>
      ((m1,m2),
      Merge(
        n1,n2,p1,p2,w1,w2,w1221,q1,q2,
        CommunityDetection.calDeltaL(
          network, n1,n2,p1,p2, w1221, qi_sum,q1,q2 ))
      )
    }
  }

  /***************************************************************************
   * trim RDD lineage and force evaluation
   ***************************************************************************/
  def trim( loop: Int, graph: Graph,
    network: Network, mergeList: RDD[((Long,Long),Merge)] ): Unit = {
   if( loop%10 == 0 ) {
     graph.vertices.localCheckpoint
      val force1 = graph.vertices.count
      graph.edges.localCheckpoint
      val force2 = graph.edges.count
      network.vertices.localCheckpoint
      val force3 = network.vertices.count
      network.edges.localCheckpoint
      val force4 = network.edges.count
      mergeList.localCheckpoint
      val force5 = mergeList.count
   }
   {}
  }

  /***************************************************************************
   * loop termination routine
   ***************************************************************************/
  def terminate( loop: Int, logFile: LogFile,
    graph: Graph, network: Network ) = {
    logFile.write( s"Merging terminates after ${loop} merges", false )
    logFile.close
    ( graph, network )
  }

  /***************************************************************************
   * given the list of possible merges,
   * return the merge and associated quantities that reduces codelength most
   ***************************************************************************/
  def findMerge( mergeList: RDD[((Long,Long),Merge)] ) = {
    mergeList.reduce {
      case (
        ((merge1A,merge2A),
          Merge(n1A,n2A,p1A,p2A,w1A,w2A,w12A,q1A,q2A,dLA)),
        ((merge1B,merge2B),
          Merge(n1B,n2B,p1B,p2B,w1B,w2B,w12B,q1B,q2B,dLB))
      )
      => {
        if( dLA < dLB )
          ((merge1A,merge2A),
            Merge(n1A,n2A,p1A,p2A,w1A,w2A,w12A,q1A,q2A,dLA))
        else if( dLA > dLB )
          ((merge1B,merge2B),
            Merge(n1B,n2B,p1B,p2B,w1B,w2B,w12B,q1B,q2B,dLB))
        else if( merge1A < merge1B )
          ((merge1A,merge2A),
            Merge(n1A,n2A,p1A,p2A,w1A,w2A,w12A,q1A,q2A,dLA))
        else
          ((merge1B,merge2B),
            Merge(n1B,n2B,p1B,p2B,w1B,w2B,w12B,q1B,q2B,dLB))
      }
    }
  }

  /***************************************************************************
   * new graph has updated partitioning
   ***************************************************************************/
  def calNewGraph( graph: Graph, merge: ((Long,Long),Merge) ) = {
    Graph(
      graph.vertices.map {
        case (idx,(name,module)) =>
          if( module==merge._1._1 || module==merge._1._2 )
            (idx,(name,merge._1._2))
          else
            (idx,(name,module))
      },
      graph.edges
    )
  }

  /***************************************************************************
   * new graph has updated partitioning
   ***************************************************************************/
  def updateMergeList(
    merge: ((Long,Long),Merge),
    mergeList: RDD[((Long,Long),Merge)],
    network: Network,
    qi_sum: Double
  ) = {
    // grab new modular properties
    val merge1 = merge._1._1
    val merge2 = merge._1._2
    val N12 = merge._2.n1 +merge._2.n2
    val P12 = merge._2.p1 +merge._2.p2
    val W12 = merge._2.w1 +merge._2.w2 -merge._2.w1221
    val Q12 = CommunityDetection.calQ(
      network.nodeNumber, N12, P12, network.tele, W12 )

    mergeList.filter {
      // delete the merged edge, ie, (merge1,merge2)
      case ((m1,m2),_) => !( m1==merge1 && m2==merge2 )
    }
    .map {
      // entries associated to merge2 now is associated to merge1
      // and put in newly merged quantities to replace old quantities
      // anyway dL always needs to be recalculated
      case ((m1,m2),Merge(n1,n2,p1,p2,w1,w2,w1221,q1,q2,_)) =>
        if( m1==merge1 || m2==merge2 )
          ((merge1,m2),Merge(N12,n2,P12,p2,W12,w2,W12+w2-w1221,Q12,q2,0.0))
        else if( m2==merge1 )
          ((m1,merge1),Merge(n1,N12,p1,P12,w1,W12,w1+W12-w1221,q1,Q12,0.0))
        else if( m2==merge2 ) {
          if( merge1 < m1 )
            ((merge1,m1),Merge(N12,n1,P12,p1,W12,w1,W12+w1-w1221,Q12,q1,0.0))
          else
            ((m1,merge1),Merge(n1,N12,p1,P12,w1,W12,w1+W12-w1221,q1,Q12,0.0))
        }
        else
          ((m1,m2),Merge(n1,n2,p1,p2,w1,w2,w1221,q1,q2,0.0))
    }
    // aggregate inter-modular connection weights
    .reduceByKey {
      case (
        Merge(n1,n2,p1,p2,w1,w2,w12,q1,q2,_),
        Merge(_,_,_,_,_,_,w21,_,_,_)
      )
      => Merge(n1,n2,p1,p2,w1,w2,w12+w21,q1,q2,0.0)
    }
    // calculate dL
    .map {
      case (
        (m1,m2),
        Merge(n1,n2,p1,p2,w1,w2,w1221,q1,q2,_)
      ) => (
        (m1,m2),
        Merge(n1,n2,p1,p2,w1,w2,w1221,q1,q2,
            CommunityDetection.calDeltaL(network,
              n1,n2,p1,p2,w1221,qi_sum,q1,q2))
      )
    }
  }

  /***************************************************************************
   * new network has updated 
   ***************************************************************************/
  def calNewNetwork( network: Network, merge: ((Long,Long),Merge) ) = {
    val newVertices = {
      // calculate properties of merged module
      val n12 = merge._2.n1 +merge._2.n2
      val p12 = merge._2.p1 +merge._2.p2
      val w12 = merge._2.w1 +merge._2.w2 -merge._2.w1221
      val q12 = CommunityDetection.calQ(
        network.nodeNumber, n12, p12, network.tele, w12 )

      // delete merged module
      network.vertices.filter {
        case (idx,_) => idx != merge._1._2
      }
      // put in new modular properties for merged module
      .map {
        case (idx,(n,p,w,q)) =>
          if( idx == merge._1._1 )
            (idx,(n12,p12,w12,q12))
          else
            (idx,(n,p,w,q))
      }
    }

    val newEdges = {
      val m1 = merge._1._1
      val m2 = merge._1._2

      network.edges
      // delete merged edges
      .filter {
        case (from,(to,_)) =>
          !( ( from==m1 && to==m2 ) || ( from==m2 && to==m1 ) )
      }
      .map {
        case (from,(to,weight)) => 
          if( from == m2 ) {
            if( m1 < to )
              ((m1,to),weight)
            else
              ((to,m1),weight)
          }
          else if( to == m2 ) {
            if( m1 < from )
              ((m1,from),weight)
            else
              ((from,m1),weight)
          }
          else
            ((from,to),weight)
      }
      // aggregate
      .reduceByKey(_+_)
      .map {
        case ((from,to),weight) => (from,(to,weight))
      }
    }

    Network(
      network.nodeNumber, network.tele,
      newVertices, newEdges,
      network.probSum,
      network.codelength +merge._2.dL
    )
  }

  /***************************************************************************
   * calculate qi_sum via dynamic progamming
   * so no RDD calculations are required
   ***************************************************************************/
  def cal_qi_sum(
    network: Network, merge: ((Long,Long),Merge), qi_sum: Double )
  = {
    val n12 = merge._2.n1 +merge._2.n2
    val p12 = merge._2.p1 +merge._2.p2
    val w12 = merge._2.w1 +merge._2.w2 -merge._2.w1221
    val q12 = CommunityDetection.calQ(
      network.nodeNumber, n12, p12, network.tele, w12 )
    val q1 = merge._2.q1
    val q2 = merge._2.q2
    qi_sum +q12 -q1 -q2
  }
}
