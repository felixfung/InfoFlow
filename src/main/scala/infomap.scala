/*****************************************************************************
 * ORIGINAL INFOMAP ALGORITHM
 * the function to partition of nodes into modules based on
 * greedily merging the pair of modules that gives
 * the greatest code length reduction
 * until code length is minimized
 *****************************************************************************/

class InfoMap extends CommunityDetection
{
  /***************************************************************************
   * case class to store all associated quantities of a potential merge
   * format of each entry is
   * (
   *   (index1,index2),
   *   (n1,n2,p1,p2,w1,w2,w1221,q1,q2,DeltaL12)
   * )
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
    ( m1: Long, m2: Long ),
    (
      n1: Long, n2: Long, p1: Long, p2: Long,
      w1: Long, w2: Long, w12: Long,
      q1: Long, q2: Long, dL: Long
    )
  )

  def apply( graph: Graph, network: Network, logFile: LogFile )
  : ( Graph, Network ) = {
    @scala.annotation.tailrec
    def recursiveMerge(
      loop: Long,
      qi_sum: Double,
      graph: Graph,
      network: Network,
      mergeList: RDD[Merge]
    ): ( Graph, Network ) = {

      trim( loop, graph, network, mergeList )

      logFile.write(s"State $loop: code length ${network.codelength}\n",false)
      logFile.save( network.graph, graph, true, "net"+loop.toString )

      if( mergeList.count == 0 )
        return terminate( loop, graph, network )

      val merge: Merge = findMerge(mergeList)
      if( merge.dL > 0 )
        return terminate( loop, graph, network )

      logFile.write(
        s"Merge $loop: merging modules $m1 and $m2"
        +s" with code length reduction ${merge.dL}\n",
      false )

      val newGraph = calNewGraph( graph, merge )
      val newMergeList = updateMergeList( merge, mergeList, network, qi_sum )
      val newCodelength = network.codelength +merge.dL
      val newNetwork = calNewNetwork( network, merge )
      val new_qi_sum = cal_qi_sum( network, merge )

      recursiveMerge( loop+1, new_qi_sum, newGraph, newNetwork, newMergeList )
    }

  /***************************************************************************
   * below are calculation functions
   ***************************************************************************/

  /***************************************************************************
   * grab all edges and aggregate into nondirectional edges
   * store all associated modular properties
   * and calculate deltaL
   ***************************************************************************/
  def genMergeList( network: Network, qi_sum: Double ): RDD[Merge] = {
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
      case (m2,((m1,n1,p1,w1,q1,w1221),(n2,p2,w2,q2)))
      Merge(
        (m1,m2),
        (n1,n2,p1,p2,w1,w2,w12,q1,q2,
        CommunityDetection.calDeltaL(
          network, n1,n2,p1,p2, w1221, qi_sum,q1,q2 ))
      )
    }
  }

  /***************************************************************************
   * trim RDD lineage and force evaluation
   ***************************************************************************/
  def trim( loop: Int, graph: Graph,
    network: Network, mergeList: RDD[Merge] ): Unit = {
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
  def terminate( loop: Int, graph: Graph, network: Network ) = {
    logFile.write( s"Merging terminates after ${loop} merges" )
    logFile.close
    ( graph, network )
  }

  /***************************************************************************
   * given the list of possible merges,
   * return the merge and associated quantities that reduces codelength most
   ***************************************************************************/
  def findMerge( mergeList: RDD[Merge] ) = {
    mergeList.reduce {
      case (
        Merge(
          (merge1A,merge2A),
          (n1A,n2A,p1A,p2A,w1A,w2A,w12A,q1A,q2A,dLA)
        ),
        Merge(
          (merge1B,merge2B),
          (n1B,n2B,p1B,p2B,w1B,w2B,w12B,q1B,q2B,dLB)
        )
      )
      => {
        if( dLA < dLB )
          ((merge1A,merge2A),
            (n1A,n2A,p1A,p2A,w1A,w2A,w12A,q1A,q2A,dLA))
        else if( dLA > dLB )
          ((merge1B,merge2B),
            (n1B,n2B,p1B,p2B,w1B,w2B,w12B,q1B,q2B,dLB))
        else if( merge1A < merge1B )
          ((merge1A,merge2A),
            (n1A,n2A,p1A,p2A,w1A,w2A,w12A,q1A,q2A,dLA))
        else
          ((merge1B,merge2B),
            (n1B,n2B,p1B,p2B,w1B,w2B,w12B,q1B,q2B,dLB))
      }
    }
  }

  /***************************************************************************
   * new graph has updated partitioning
   ***************************************************************************/
  def calNewGraph( graph: Graph, merge: Merge ) = {
    Graph(
      graph.vertices.map {
        case (idx,(name,module)) =>
          if( module==merge.m1 || module==merge.m2 )
            (idx,(name,merge.m12))
          else
            (idx,name,module)
      },
      graph.edges
    )
  }

  /***************************************************************************
   * new graph has updated partitioning
   ***************************************************************************/
  def updateMergeList(
    merge: Merge,
    mergeList: RDD[Merge],
    network: Network,
    qi_sum
  ) = {
    table.filter {
      // delete the merged edge, ie, (merge1,merge2)
      case Merge((m1,m2),_) => !( m1==merge.m1 && m2==merge.m2 )
    }
    .map {
      // entries associated to merge2 now is associated to merge1
      // and put in newly merged quantities to replace old quantities
      // anyway dL always needs to be recalculated
      case Merge((m1,m2),(ni,nj,pi,pj,wi,wj,wij,qi,qj,_)) =>
        if( m1==merge.m1 || m2==merge.m2 )
          Merge((merge.m1,m2),(n12,nj,p12,pj,w12,wj,wij,q12,qj,0.0))
        else if( m2==merge1 )
          Merge((m2,merge.m1),(ni,n12,pi,p12,wi,w12,wij,qi,q12,0.0))
        else if( m2==merge.m2 ) {
          if( merge.m1 < m1 )
            Merge((merge.m1,m1),(n12,ni,p12,pi,w12,wi,wij,q12,qi,0.0))
          else
            Merge((m1,merge.m1),(ni,n12,pi,p12,wi,w12,wij,qi,q12,0.0))
        }
        else
          Merge((m1,m2),(ni,nj,pi,pj,wi,wj,wij,qi,qj,deltaLiij))
    }
    // aggregate inter-modular connection weights
    .reduceByKey {
      case (
        (n1,n2,p1,p2,w1,w2,w12,q1,q2,_),
        (_,_,_,_,_,_,w21,_,_,_)
      )
      => (n1,n2,p1,p2,w1,w2,w12+w21,q1,q2,0.0)
    }
    // calculate dL
    .map {
      case Merge(
        (m1,m2),
        ( (n1,n2,p1,p2,w1,w2,w1221,q1,q2,_) )
      ) =>
        if( m1==merge.m1 || m2==merge.m1 )
          Merge(
            (m1,m2),
            ((n1,n2,p1,p2,w1,w2,w1221,q1,q2,
                CommunityDetection.calDeltaL(network,
                  n1,n2,p1,p2,w1221,qi_sum,q1,q2)))
          )
    }
  }

  /***************************************************************************
   * new network has updated 
   ***************************************************************************/
  def calNewNetwork( network: Network, merge: Merge ) = {
    val newVertices = {
      // calculate properties of merged module
      val n12 = merge.n1 +merge.n2
      val p12 = merge.p1 +merge.p2
      val w12 = merge.w1 +merge.w2 -merge.w1221
      val q12 = CommunityDetection.calQ(
        network.nodeNumber, n12, p12, network.tele, w12 )

      // delete merged module
      network.vertices.filter {
        case (idx,_) => idx != merge.m2
      }
      // put in new modular properties for merged module
      .map {
        case (idx,(n,p,w,q)) =>
          if( idx == merge.m1 )
            (idx,(n12,p12,w12,q12))
          else
            (idx,(n,p,w,q))
      }
    }

    val newEdges = {
      val m1 = merge.m1
      val m2 = merge.m2

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
        case ((from,to),weight) =>
      }
    }

    Network(
      network.nodeNumber, network.tele,
      newVertices, newEdges,
      network.probSum, network.codelength
    )
  }

  /***************************************************************************
   * calculate qi_sum via dynamic progamming
   * so no RDD calculations are required
   ***************************************************************************/
  def cal_qi_sum( network: Network, merge: Merge ) = {
    val n12 = merge.n1 +merge.n2
    val p12 = merge.p1 +merge.p2
    val w12 = merge.w1 +merge.w2 -merge.w1221
    val q12 = CommunityDetection.calQ(
      network.nodeNumber, n12, p12, network.tele, w12 )
    qi_sum +q12 -q1 -q2
  }

  /***************************************************************************
   * invoke recursive merging calls
   ***************************************************************************/
    val qi_sum = network.vertices.map {
      case (_,(_,_,_,q)) => q
    }.sum
    val edgeList = genEdgeList( network, qi_sum )
    recursiveMerge( 0, qi_sum, graph, network, edgeList )
  }
}
