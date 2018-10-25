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
  {
    ( from: Long, to: Long ),
    (
      n1: Long, n2: Long, p1: Long, p2: Long,
      w1: Long, w2: Long, w12: Long,
      q1: Long, q2: Long, dL: Long
    )
  }

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

      if( loop%10 == 0 ) trim( graph, network, mergeList )

      logFile.write(s"State $loop: code length ${network.codelength}\n",false)
      logFile.save( network.graph, graph, true, "net"+loop.toString )

      if( mergeList.count == 0 )
        return terminate( loop, graph, network )

      val merge: Merge = findMerge(mergeList)
      val 
      if( deltaL > 0 )
        return terminate( loop, graph, network )

      val m12 = if( m1<m2 ) m1 else m2
      val n12 = n1 +n2
      val p12 = p1 +p2
      val w12 = w1 +w2 -w1221
      val q12 = CommunityDetection.calQ( nodeNumber, n12, p12, tele, w12 )

      logFile.write(
        s"Merge $loop: merging modules $m1 and $m2"
        +s" with code length reduction $deltaL\n",
      false )

      val newGraph = calNewGraph( graph, merge )
      val newMergeList = updateMergeList(merge)
      val newCodelength = network.codelength +merge.deltaL
      val newNetwork = calNewNetwork( network, merge )
      val new_qi_sum = qi_sum +q12 -q1 -q2
      recursiveMerge( loop+1, new_qi_sum, newGraph, newNetwork, newMergeList )
    }

  /***************************************************************************
   * below are calculation functions
   ***************************************************************************/

  def genMergeList( network: Network ): RDD[Merdge] = {
    // sum of q's
    // used for deltaL calculations
    val qi_sum = network.vertices.map {
      case (_,(_,_,_,q)) => q
    }
    .sum

    // the reverse edge weights are needed to calculate deltaL
    val reverseEdges = network.edges.map {
      case (m1,(m2,weight)) => ((m2,m1),weight)
    }

    // grab all edges (and merge with possible reversals)
    // store all associated modular properties
    // and calculate deltaL
    network.edges.map.join( network.vertices ).map {
      case (m1,((m2,w12),(n1,p1,w1,q1))) => (m2,(m1,n1,p1,w1,q1,w12))
    }
    .join( network.vertices ).map {
      case (m2,((m1,n1,p1,w1,q1,w12),(n2,p2,w2,q2)))
      => ((m1,m2),(n1,n2,p1,p2,w1,w2,w12,q1,q2))
    }
    .leftOuterJoin( reverseEdges ).map {
      case ((m1,m2),((n1,n2,p1,p2,w1,w2,w12,q1,q2),Some(w21))) =>
      Merge(
        (m1,m2),
        (n1,n2,p1,p2,w1,w2,w12,q1,q2,
        CommunityDetection.calDeltaL( network, n1,n2,p1,p2,
          w12+w21, // two way prob. going between m1 and m2
        qi_sum,q1,q2 ))
      )
      case ((m1,m2),((n1,n2,p1,p2,w1,w2,w12,q1,q2),None)) =>
      Merge(
        (m1,m2),
        (n1,n2,p1,p2,w1,w2,w12,q1,q2,
        CommunityDetection.calDeltaL( network, n1,n2,p1,p2,w12,qi_sum,q1,q2 ))
      )
    }
  }

  /***************************************************************************
   * trim RDD lineage and force evaluation
   ***************************************************************************/
  def trim = {
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
    mergeList.reduce
    val( (merge1,merge2), (n1,n2,p1,p2,w1,w2,w102,q1,q2,deltaLi12) )
      = table.reduce {
      case (
        (
          (merge1A,merge2A),
          (n1A,n2A,p1A,p2A,w1A,w2A,w12A,q1A,q2A,deltaLi12A)
        ),
        (
          (merge1B,merge2B),
          (n1B,n2B,p1B,p2B,w1B,w2B,w12B,q1B,q2B,deltaLi12B)
        )
      )
      => {
        val q12A = Partition.calQ(
          nodeNumber, n1A+n2A, p1A+p2A, tele,
          w1A+w2A-w12A
        )
        val q12B = Partition.calQ(
          nodeNumber, n1B+n2B, p1B+p2B, tele,
          w1B+w2B-w12B
        )
        val deltaLA = InfoMap.calDeltaL(deltaLi12A,qi_sum,q1A,q2A,q12A)
        val deltaLB = InfoMap.calDeltaL(deltaLi12B,qi_sum,q1B,q2B,q12B)
        if( deltaLA < deltaLB )
           ((merge1A,merge2A),
             (n1A,n2A,p1A,p2A,w1A,w2A,w12A,q1A,q2A,deltaLi12A))
         else
           ((merge1B,merge2B),
             (n1B,n2B,p1B,p2B,w1B,w2B,w12B,q1B,q2B,deltaLi12B))
      }
    }
  }

  /***************************************************************************
   * calculate properties related to the newly merged module
   ***************************************************************************/
  def calMergedProps(
    nodeNumber: Long, tele: Double,
    m1: Long, m2: Long, n1: Long, n2: Long,
    p1: Double, p2: Double, w1: Double, w2: Double, w1221: Double
  ) = {
    (m12,n12,p12,w12,q12)
  }

  def calNewGraph( graph: Graph, m1: Long, m2: Long, m12: Long ) = {
    Graph(
      graph.vertices.map {
        case (idx,(name,module)) =>
          if( module==m1 || module==m2 ) (idx,(name,m12))
          else (idx,name,module)
      },
      graph.edges
    )
  }

  def updateEdgeList(
    edgeList: RDD[EdgeList],
    m1: Long, m2: Long, m12: Long,
    n12: Long, p12: Double, w12: Double, q12: Double
  ) = {
          val newTable = table.filter {
            // delete the merged edge, ie, (merge1,merge2)
            case ((from,to),_) => from!=merge1 || to!=merge2
          }
          .map {
            // entries associated to merge2 now is associated to merge1
            // and put in newly merged quantities to replace old quantities
            case ((from,to),(ni,nj,pi,pj,wi,wj,wij,qi,qj,deltaLiij)) =>
              if( from==merge1 || from==merge2 )
                ((merge1,to),(n12,nj,p12,pj,w12,wj,wij,q12,qj,0.0))
              else if( to==merge1 )
                  ((from,merge1),(ni,n12,pi,p12,wi,w12,wij,qi,q12,0.0))
              else if( to==merge2 ) {
                if( merge1 < from )
                  ((merge1,from),(n12,ni,p12,pi,w12,wi,wij,q12,qi,0.0))
                else
                  ((from,merge1),(ni,n12,pi,p12,wi,w12,wij,qi,q12,0.0))
              }
              else
                ((from,to),(ni,nj,pi,pj,wi,wj,wij,qi,qj,deltaLiij))
          }
          .reduceByKey { // aggregate inter-modular connection weights
            case (
              (ni,nj,pi,pj,wi,wj,wij,qi,qj,deltaLiij),
              (_,_,_,_,_,_,wji,_,_,_)
            )
            => (ni,nj,pi,pj,wi,wj,wij+wji,qi,qj,deltaLiij)
          }
          .map {
            case (
              (from,to),
              ( (ni,nj,pi,pj,wi,wj,wij,qi,qj,deltaLiij) )
            ) =>
              // fill with newly merged modular properties
              // and calculate q_ij, deltaL_ij
              if( from==merge1 || to==merge1 )
                (
                  (from,to),
                  InfoMap.tableEntry(
                    nodeNumber,tele,qi_sum,ni,nj,pi,pj,wi,wj,wij,qi,qj
                  )
                )
              // unrelated to newly merged module, leave unchanged
              else
                (
                  (from,to),
                  (ni,nj,pi,pj,wi,wj,wij,qi,qj,deltaLiij)
                )
          }
  }

  def calNewNetwork( network: Network, m1: Long, m2: Long, m12: Long ) = {
    val mMod = m12
    val mDel = if( m1==mMod ) m2 else m1
    ...
  }

  /***************************************************************************
   * invoke recursive merging calls
   ***************************************************************************/
    val qi_sum = network.vertices.map {
      case (_,(_,_,_,q)) => q
    }.sum
    recursiveMerge( 0, qi_sum, graph, network, genEdgeList(network) )
  }
}
