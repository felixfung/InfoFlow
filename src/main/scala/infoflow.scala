/*****************************************************************************
 * InfoFlow community detection algorithm
 *
 * this is the multimerging algorithm
 * where each module merges with another module
 * that gives the greatest reduction in the code length
 * so that the following may happen for a module:
 *   (1) it seeks no merge, because of no connections with other modules,
 *       or all potential merges increase code length
 *   (2) it seeks a merge with another module (which also seeks a merge w/ it)
 *   (3) it seeks a merge with another, which seeks merge with some other
 *       module, and so on; in which case we merge all these modules
 *
 * this is a big file, organized so that:
 * it contains a simple class with only one function, apply()
 * which contains only a tail recursive function
 * this tail recursive function contains many calculated quantities
 * often one quantity's calculation involves the other, forming a DAG
 * the calculations are organized so that
 * each each calculation is defined within a function
 * and called in the main routine
 *****************************************************************************/

import org.apache.spark.rdd.RDD

class InfoFlow extends CommunityDetection
{
  def apply( graph: Graph, network: Network, logFile: LogFile )
  : ( Graph, Network ) = {
    @scala.annotation.tailrec
    def recursiveMerge(
      loop: Int,
      graph: Graph,
      network: Network
    ): ( Graph, Network ) = {

      logFile.write( s"State $loop: code length ${network.codelength}\n", false)
      logFile.save( graph, network, true, "0" )

      trim( loop, graph, network )

      val deltaL = calDeltaL(network)
      val m2Merge = calm2Merge(deltaL)
      m2Merge.cache
      if( m2Merge.count == 0 )
        return terminate( loop, graph, network )

      val moduleMap = calModuleMap( network, m2Merge )
      val newGraph = calNewGraph( moduleMap, graph )
      val newNetwork = calNewNetwork( moduleMap, network )

      if( newNetwork.codelength >= network.codelength )
        return terminate( loop, graph, network )

      logFile.write(
        s"Merge ${loop+1}: merging ${network.vertices.count}"
        +s" modules into ${newNetwork.vertices.count} modules\n",
        false
      )

      recursiveMerge( loop+1, newGraph, newNetwork )
    }

  /***************************************************************************
   * below are functions to perform calculations
   ***************************************************************************/

    def trim( loop: Int, graph: Graph, network: Network ) = {
      if( loop%10 == 0 ) {
        network.vertices.localCheckpoint
        val count1 = network.vertices.count
        network.edges.localCheckpoint
        val count2 = network.edges.count
        graph.vertices.localCheckpoint
        val count3 = graph.vertices.count
        graph.edges.localCheckpoint
        val count4 = graph.edges.count
      }
    }

    def terminate( loop: Long, graph: Graph, network: Network ) = {
      logFile.write( s"Merging terminates after $loop merges\n", false )
      ( graph, network )
    }

  /***************************************************************************
   * calculate the deltaL table for all possible merges
   * | src , dst , dL |
   ***************************************************************************/
    def calDeltaL( network: Network ): RDD[(Long,(Long,Double))] = {
      val qi_sum = network.vertices.map {
        case (_,(_,_,_,q)) => q
      }
      .sum

      val reverseEdges = network.edges.map {
        case (from,(to,weight)) => ((to,from),weight)
      }

      network.edges.join( network.vertices ).map {
        case (m1,((m2,w12),(n1,p1,w1,q1))) => (m2,(m1,n1,p1,w1,q1,w12))
      }
      .join( network.vertices ).map {
        case (m2,((m1,n1,p1,w1,q1,w12),(n2,p2,w2,q2))) =>
          ((m1,m2),(n1,n2,p1,p2,w1,w2,q1,q2,w12))
      }
      .leftOuterJoin( reverseEdges ).map {
        case ((m1,m2),((n1,n2,p1,p2,w1,w2,q1,q2,w12),Some(w21))) =>
          //println(s"${network.nodeNumber} ($m1,$m2): ($n1,$n2) ($p1,$p2) ($w1 $w2 $w12 $w21) ($q1 $q2 ${CommunityDetection.calQ(network.nodeNumber,n1+n2,p1+p2,network.tele,w1+w2-w12-w21)} $qi_sum) => ${CommunityDetection.calDeltaL(network,n1,n2,p1,p2,w1+w2-w12-w21,qi_sum,q1,q2)}")
          (
            m1,(m2,
            CommunityDetection.calDeltaL(
              network,
              n1, n2, p1, p2,
              w1+w2-w12-w21,
              qi_sum, q1, q2
            ))
          )
        case ((m1,m2),((n1,n2,p1,p2,w1,w2,q1,q2,w12),None)) =>
          //println(s"${network.nodeNumber} ($m1,$m2): ($n1,$n2) ($p1,$p2) ($w1 $w2 $w12 0) ($q1 $q2 ${CommunityDetection.calQ(network.nodeNumber,n1+n2,p1+p2,network.tele,w1+w2-w12)} $qi_sum) => ${CommunityDetection.calDeltaL(network,n1,n2,p1,p2,w1+w2-w12,qi_sum,q1,q2)}")
          (
            m1,(m2,
            CommunityDetection.calDeltaL(
              network,
              n1, n2, p1, p2,
              w1+w2-w12,
              qi_sum, q1, q2
            ))
          )
      }
    }
    // importantly, dL is symmetric towards src and dst
    // so if both edges (src,dst) and (dst,src) exists
    // their dL would be identical
    // since dL (and the whole purpose of this table)
    // is used to decide on merge, there is an option
    // on whether a module could seek merge with another
    // if there is an opposite connection
    // eg a graph like this: m0 <- m1 -> m2 -> m3
    // m2 seeks to merge with m3
    // m1 might merge with m0
    // BUT the code length reduction if m2 seeks merge with m1
    // is greater than that of m2 seeking merge with m3
    // the question arise, should such a merge (opposite direction to edge),
    // be considered?
    // this dilemma stems from how edges are directional
    // while merges are non-directional, symmetric towards two modules
    // in the bigger picture, this merge seeking behaviour
    // is part of a greedy algorithm
    // so that the best choice is heuristic based only
    // to keep things simple, don't consider opposite edge merge now

  /***************************************************************************
   * each module seeks to merge with another connected module
   * which would offer the greatest reduction in code length
   * this forms (weakly) connected components of merged modules
   * to be generated later
   *
   * this implementation has two subtleties:
   *   (1) merge seeks are directional
   *       so that m2 will never seek to merge with m1 if only (m1,m2) exists
   *   (2) (m1,m2), (m1,m3) has identical code length reduction
   *       then both merges are selected
   *       that is, m1 seeks to merge with both m2 and m3
   * |module , module to seek merge to |
   ***************************************************************************/
    def calm2Merge( deltaL: RDD[(Long,(Long,Double))] )
    : RDD[(Long,Long)]= {
      // module to merge
      // (module,module to seek merge to)

      // obtain minimum dL for each source vertex
      deltaL.reduceByKey {
        case ( (_,dL1), (_,dL2) ) => if( dL1 < dL2 ) (0,dL1) else (0,dL2)
      }
      .map {
        case (idx,(_,dL)) => (idx,dL)
      }
      // for each source vertex, retain all that has dL==minimum dL
      // all others will be filtered away by setting dL=1
      .join( deltaL ).map {
        case (m1,(dL_min,(m2,dL))) =>
          if( dL==dL_min ) (m1,(m2,dL)) else (m1,(m2,1.0))
      }
      // filter away all non-minimum dL and positive dL
      .filter {
        case (m1,(m2,dL)) => dL<0
      }
      // take away dL info
      .map {
        case (m1,(m2,_)) => (m1,m2)
      }
    }

  /***************************************************************************
   * map each network.vertices to a new module
   * according to connected components of m2Merge
   * | id , module |
   ***************************************************************************/
    def calModuleMap( network: Network, m2Merge: RDD[(Long,Long)] )
    : RDD[(Long,Long)] = {
      val labeledEdges: RDD[((Long,Long),Long)] = InfoFlow.labelEdges(m2Merge)
      labeledEdges.flatMap {
        case ((m1,m2),module) => Seq( (m1,module), (m2,module) )
      }
      .distinct
      .filter {
        case (from,to) => from!=to
      }
    }

  /***************************************************************************
   * calculate new nodal-modular partitioning scheme
   * difference from moduleMap:
   *   (1) moduleMap vertices are modules
   *       newPartition nodes are all original nodes
   *   (2) moduleMap used only within this function for further calculations
   *       newPartition saved to graph, which is part of final result
   * | id , module |
   ***************************************************************************/
    def calNewGraph( moduleMap: RDD[(Long,Long)], graph: Graph )
    : Graph = {
      val newVertices = graph.vertices.map {
        case (idx,(name,module)) => (module,(idx,name))
      }
      .leftOuterJoin( moduleMap )
      .map {
        case (oldModule,((idx,name),Some(newModule))) => (idx,(name,newModule))
        case (oldModule,((idx,name),None)) => (idx,(name,oldModule))
      }
      Graph( newVertices, graph.edges )
    }

    def calNewNetwork(
      moduleMap: RDD[(Long,Long)], network: Network
    ) = {

    /*************************************************************************
     * intermediate edges
     * map the associated modules into new module indices
     * if the new indices are the same, they are intramodular connections
     * and will be subtracted from the w_i's
     * if the new indices are different, they are intermodular connections
     * and will be aggregated into w_ij's
     * | src , dst , iWj |
     *************************************************************************/
      def calInterEdges(
        network: Network, moduleMap: RDD[(Long,Long)]
      ): RDD[(Long,(Long,Double))] = {
        network.edges.leftOuterJoin( moduleMap ).map {
          case (from,((to,weight),Some(newFrom))) => (to,(newFrom,weight))
          case (from,((to,weight),None)) => (to,(from,weight))
        }
        .leftOuterJoin( moduleMap ).map {
          case (to,((newFrom,weight),Some(newTo))) =>
            ((newFrom,newTo),weight)
          case (to,((newFrom,weight),None)) =>
            ((newFrom,to),weight)
        }
        .reduceByKey(_+_)
        .map {
          case ((from,to),weight) => (from,(to,weight))
        }
      }

    /*************************************************************************
     * modular properties calculations
     *************************************************************************/
      def calNewModules(
        network: Network, moduleMap: RDD[(Long,Long)],
        interEdges: RDD[(Long,(Long,Double))]
      ): RDD[(Long,(Long,Double,Double,Double))] = {
        // aggregate size, prob, exitw over the same modular index
        // for size and prob, that gives the final result
        // for exitw, we have to subtract intramodular edges in the next step
        val sumOnly = network.vertices.leftOuterJoin(moduleMap).map {
          case (module,((n,p,w,_),Some(newModule)))
            => (newModule,(n,p,w))
          case (module,((n,p,w,_),None))
            => (module,(n,p,w))
        }
        .reduceByKey {
          case ( (n1,p1,w1), (n2,p2,w2) ) => (n1+n2,p1+p2,w1+w2)
        }

        // subtract w12 from the sum of w's
        interEdges.filter {
          case (from,(to,w12)) => from==to
        }
        .map {
          case (from,(to,w12)) => (from,w12)
        }
        .reduceByKey(_+_)
        .rightOuterJoin(sumOnly).map {
          case (module,(Some(w12),(n,p,w)))
          => ( module,( n, p, w-w12,
            CommunityDetection.calQ( network.nodeNumber, n, p,
              network.tele, w-w12 )
          ))
          case (module,(None,(n,p,w)))
          => ( module,( n, p, w,
            CommunityDetection.calQ( network.nodeNumber, n, p,
              network.tele, w )
          ))
        }
      }

      val interEdges = calInterEdges( network, moduleMap )
      interEdges.cache
      val newModules = calNewModules( network, moduleMap, interEdges )
      newModules.cache
      val newEdges = interEdges.filter { case (from,(to,_)) => from != to }
      val newCodelength = CommunityDetection.calCodelength(
        newModules, network.probSum )

      Network(
        network.nodeNumber, network.tele,
        newModules, newEdges,
        network.probSum, newCodelength
      )
    }

    recursiveMerge( 0, graph, network )
  }
}

/*****************************************************************************
 * given an RDD of edges,
 * partition the edges according to the connected components
 * and label each edge by the lowest vertex index of the connected component
 *****************************************************************************/
object InfoFlow
{
  def labelEdges( edge2label: RDD[(Long,Long)] ): RDD[((Long,Long),Long)] = {

  if( edge2label.isEmpty )
    throw new Exception("Empty RDD argument")

  /***************************************************************************
   * initial condition
   ***************************************************************************/
    // number of times each vertex appears in the edges
    // (vertex,count)
    val vertexCount = edge2label.flatMap {
      case (from,to) => Seq( (from,1), (to,1) )
    }
    .reduceByKey(_+_)

    // labeled edges, 0th iteration
    // ((idx1,idx2),label)
    val labelEdge1 = edge2label.join(vertexCount).map {
      case (from,(to,fromCount)) => (to,(from,fromCount))
    }
    .join(vertexCount).map {
      case (to,((from,fromCount),toCount)) =>
        if( fromCount > toCount )
          ((from,to),from)
        else if( fromCount < toCount )
          ((from,to),to)
        else if( from < to )
          ((from,to),from)
        else
          ((from,to),to)
    }

  /***************************************************************************
   * recursive function
   ***************************************************************************/
    def labelEdge( labelEdge1: RDD[((Long,Long),Long)] )
    : RDD[((Long,Long),Long)] = {

      val labelCount = labelEdge1.map {
        case ((from,to),label) => (label,1)
      }
      .reduceByKey(_+_)

      val vertexLabel = labelEdge1.flatMap {
        case ((from,to),label) => Seq( (label,from), (label,to) )
      }
      .join(labelCount)
      .map {
        case (label,(vertex,labelCount)) => (vertex,(label,labelCount))
      }
      .reduceByKey {
        case ( (label1,labelCount1), (label2,labelCount2) ) =>
          if( labelCount1 > labelCount2 )
            (label1,labelCount1)
          else if( labelCount1 < labelCount2 )
            (label2,labelCount2)
          else if( label1 < label2 )
            (label1,labelCount1)
          else
            (label2,labelCount2)
      }

      val labelEdge2 = labelEdge1.map {
        case ((from,to),label) => (from,to)
      }
      .join(vertexLabel)
      .map {
        case (from,(to,(fromLabel,fromLabelCount)))
        => (to,(from,fromLabel,fromLabelCount))
      }
      .join(vertexLabel)
      .map {
        case (to,((from, fromLabel,fromCount), (toLabel,toCount))) =>
          if( fromCount > toCount )
            ((from,to),fromLabel)
          else if( fromCount < toCount )
            ((from,to),toLabel)
          else if( fromLabel < toLabel )
            ((from,to),fromLabel)
          else /* if( fromLabel >= toLabel ) */
            ((from,to),toLabel)
      }

      val equivalence = labelEdge1.join(labelEdge2).map {
        case (edge,(oldLabel,newLabel)) => oldLabel==newLabel
      }
      .reduce(_&&_)
      if( equivalence )
        labelEdge1
      else
        labelEdge( labelEdge2 )
    }

    // obtain labeled edges according to connected components
    // where the labeled is the most common vertex index within component
    val labeledEdges = labelEdge( labelEdge1 )
    .map {
      case ((from,to),label) => (label,(from,to))
    }
    labeledEdges.cache

    val nearlyProperEdges = labeledEdges.reduceByKey {
      case ( (from1,to1), (from2,to2) ) =>
        val lowestFrom = Math.min( from1, from2 )
        val lowestTo = Math.min( to1, to2 )
        val lowestIdx = Math.min( lowestFrom, lowestTo )
        (lowestIdx,lowestIdx)
    }
    .join( labeledEdges )
    .map {
      case (label,((newLabel,_),(from,to))) => (newLabel,(from,to))
    }
  
    // reduceByKey() would miss edges with singular labels
    // these code account for those
    val singularLabel = nearlyProperEdges.map {
      case (label,_) => (label,1)
    }
    .reduceByKey {
      case (count1,count2) => count1+count2
    }

    nearlyProperEdges.join(singularLabel).map {
      case (label,((from,to),count)) =>
        if( count == 1 )
          ((from,to), Math.min(from,to) )
        else
          ((from,to),label)
    }
  }
}
