import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.commons.math.util.MathUtils

import java.io._

object InfoFlow
{
  def labelEdges( edge2label: RDD[(Int,Int)] ) = {
  /***************************************************************************
   * given an RDD of edges,
   * partition the edges according to the connected components
   * and label each edge by the most common vertex of the connected component
   *
   * this static method is used in every iteration of a InFoFlow loop
   * it is declared as a static class function to enable unit testing
   ***************************************************************************/

  if( edge2label.take(1).length == 0 )
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
        else {
          if( from < to )
            ((from,to),from)
          else
            ((from,to),to)
        }
    }

  /***************************************************************************
   * recursive function
   ***************************************************************************/
    def labelEdge( labelEdge1: RDD[((Int,Int),Int)] )
    : RDD[((Int,Int),Int)] = {
      // count the edge label occurrences
      // (vertex,count)
      val vertexCount: RDD[(Int,Int)] = labelEdge1.map {
        case ((from,to),label) => (label,1)
      }
      .reduceByKey(_+_)

      // for each edge, if both vertices has nonzero vertex count,
      // produce a mapping so that
      // the vertex of lower count goes to the vertex of higher count
      val map: RDD[(Int,Int)] = labelEdge1.map {
        case ((from,to),label) => (from,to)
      }
      .join(vertexCount).map {
        case (from,(to,fromCount)) => (to,(from,fromCount))
      }
      .join(vertexCount).map {
        case (to,((from,fromCount),toCount)) =>
          if( fromCount > toCount )
            (to,(from,fromCount))
          else if( fromCount < toCount )
            (from,(to,toCount))
          else { // fromCount == toCount
            if( from < to )
              (to,(from,fromCount))
            else //if( from > to )
              (from,(to,toCount))
          }
      }
      .reduceByKey {
        // when an index is mapped into two other indices,
        // choose to map into an index with a higher vertex count
        case (
               (target1,targetCount1),
               (target2,targetCount2)
             )
        =>
          if( targetCount1 > targetCount2 )
            (target1,targetCount1)
          else if( targetCount1 < targetCount2 )
            (target2,targetCount2)
          else if( target1 < target2 )
            (target1,targetCount1)
          else
            (target2,targetCount2)
      }
      .map {
        case (idx,(target,count)) => (idx,target)
      }

      // check if mapping is empty
      if( map.take(1).length == 0 ) {
        // if mapping is empty, ie no label is to be changed, terminate
        labelEdge1
      }
      else { // otherwise keep iterating
        // merge each edge with the mapping
        // (label,((from,to),newLabel))
        val labelEdge2 = labelEdge1.map {
          case ((from,to),label) => (label,(from,to))
        }
        .leftOuterJoin(map).map {
          case (label,((from,to),Some(newLabel))) => ((from,to),newLabel)
          case (label,((from,to),None)) => ((from,to),label)
        }
        .distinct
        labelEdge( labelEdge2 )
      }
    }
    // invoke recursive function
    labelEdge( labelEdge1 )
  }

  def calDeltaL( qi_sum: Double,
    q1: Double, q2: Double, q12: Double, p1: Double, p2: Double ) = {
      val(deltaL1,deltaL2) = Partition.calDeltaL(qi_sum,q1,q2,q12,p1,p2)
      Partition.calDeltaL(deltaL2,qi_sum,deltaL1)
  }
}

class InfoFlow( outputDir: String ) extends MergeAlgo(outputDir)
{
  /***************************************************************************
   * logging system
   ***************************************************************************/
  // create output directory
  new File(outputDir).mkdirs
  // create file to store the loop of code lengths
  val logFile = new PrintWriter( new File(outputDir+"/log.txt") )

  def apply( partition: Partition ): Partition = {

    val nodeNumber = partition.nodeNumber
    val tele = partition.tele

  /***************************************************************************
   * calculate the initial deltaL
   ***************************************************************************/

    // the sum of the q's are used for the deltaL calculations
    val qi_sum = partition.modules.map {
      case (_,(_,_,_,q)) => q
    }
    .sum

    // the sum of the ergodic frequency of all nodes is needed
    // for each loop to calculate the code length
    val ergodicFreqSum = partition.modules.map {
      case (_,(_,p,_,_)) => Partition.plogp(p)
    }
    .sum

    // calculate deltaL
    val deltaL = partition.iWj.map {
      case ((m1,m2),w12) => (m1,(m2,w12))
    }
    .join(partition.modules).map {
      case (m1,((m2,w12),(n1,p1,w1,q1)))
        => (m2,(m1,n1,p1,w1,q1,w12))
    }
    .join(partition.modules).map {
      case (m2,((m1,n1,p1,w1,q1,w12),(n2,p2,w2,q2))) => {
        val q12 = Partition.calQ( nodeNumber, n1+n2, p1+p2, tele, w1+w2-w12 )
        ( (m1,m2), InfoFlow.calDeltaL(qi_sum,q1,q2,q12,p1,p2) )
      }
    }

  /***************************************************************************
   * write initial condition in log file
   ***************************************************************************/
    // log code length
    logFile.write( "State 0: code length "
      +partition.codeLength.toString +"\n" )
    // log partitioning
    partition.partitioning.saveAsTextFile( outputDir+"/partition_0" )

  /***************************************************************************
   * this is the multimerging algorithm
   * where each module merges with another module
   * that gives the greatest reduction in the code length
   * so that the following may happen for a module:
   *   (1) it seeks no merge, because of no connections with other modules,
   *       or all potential merges increase code length
   *   (2) it seeks a merge with another module (which also seeks a merge w/ it)
   *   (3) it seeks a merge with another, which seeks merge with some other
   *       module, and so on; in which case we merge all these modules
   ***************************************************************************/
    def recursiveMerge(
      loop: Int,
      partition: Partition,
      deltaL: RDD[((Int,Int),Double)]
    ): Partition = {

  /***************************************************************************
   * each module seeks to merge with another connected module
   * which would offer the greatest reduction in code length
   ***************************************************************************/
      // module to merge
      // (module,module to seek merge to)
      val m2Merge: RDD[(Int,Int)] = deltaL.flatMap {
        case ((idx1,idx2),deltaL12)
        => Seq( (idx2,(idx1,deltaL12)), (idx1,(idx2,deltaL12)) )
      }
      .reduceByKey {
        case (
               (idx2A,deltaL12A),
               (idx2B,deltaL12B)
             )
        =>
          if( deltaL12A <= deltaL12B )
            (idx2A,deltaL12A)
          else
            (idx2B,deltaL12B)
      }
      // if DeltaL is non-negative, do not seek to merge
      .filter {
        case (idx,(target,deltaL)) => deltaL < 0
      }
      // anyway take away DeltaL info
      // and rearrange the indices so that we have the smaller index first
      .map {
        case (idx,(target,deltaL)) =>
          if( idx < target ) (idx,target)
          else (target,idx)
      }
      .distinct

      // if m2Merge is empty, then no modules seek to merge
      // terminate loop
      if( m2Merge.take(1).length == 0 ) {
        logFile.write( "Merging terminates after " +(loop-1).toString +" merges" )
        logFile.close
        return partition
      }
      else {

  /***************************************************************************
   * for all inter-modular connection, assign it to a module
   ***************************************************************************/

        // labeled connection
        // ((from,to),module)
        val labeledConn: RDD[((Int,Int),Int)] = InfoFlow.labelEdges(m2Merge)

        // this map maps each old module index into a new module index
        // (moduleFrom,moduleTo)
        val moduleMap: RDD[(Int,Int)] = labeledConn.flatMap {
          case ((vertex1,vertex2),labelV) =>
            Seq( (vertex1,labelV), (vertex2,labelV) )
        }
        .distinct
        .filter {
          case (from,to) => from!=to
        }

  /***************************************************************************
   * register and log the partitioning scheme
   ***************************************************************************/

        // new nodal-modular partitioning scheme
        // (node,module)
        val newPartitioning = partition.partitioning.map {
          case (node,module) => (module,node)
        }
        .leftOuterJoin(moduleMap).map {
          case ( oldModule, (node,Some(newModule)) )
            => (node,newModule)
          case ( oldModule, (node,None) )
            => (node,oldModule)
        }

  /***************************************************************************
   * modular properties calculations
   ***************************************************************************/

        // intra-modular exit probabilities within each new module
        // (module,all exit probability within new module)
        val intraMw: RDD[(Int,Double)] = labeledConn.join(partition.iWj).map {
          case ((from,to),(module,weight)) => (module,weight)
        }
        .reduceByKey(_+_)

        // intermediate iWj
        // map the associated modules into new module indices
        // if the new indices are the same, they are intramodular connections
        // and will be subtracted from the w_i's
        // if the new indices are different, they are intemodular connections
        // and will be aggregated into w_ij's
        // ((module1,module2),iWj)
        val interiWj: RDD[((Int,Int),Double)] = partition.iWj.map {
          case ((from,to),weight) => (from,(to,weight))
        }
        .leftOuterJoin(moduleMap).map {
          case (from,((to,weight),Some(newFrom))) => (to,(newFrom,weight))
          case (from,((to,weight),None)) => (to,(from,weight))
        }
        .leftOuterJoin(moduleMap).map {
          case (to,((newFrom,weight),Some(newTo))) =>
            if( newFrom < newTo )
              ((newFrom,newTo),weight)
            else
              ((newTo,newFrom),weight)
          case (to,((newFrom,weight),None)) =>
            if( newFrom < to )
              ((newFrom,to),weight)
            else
              ((to,newFrom),weight)
        }

        // (module,(n,p,w,q))
        val newModules: RDD[(Int,(Int,Double,Double,Double))] = {
          // aggregate n,p,w over the same modular index
          // for n and p, that gives the final result
          // for w, we have to subtract w12 in the next step
          val sumOnly = partition.modules.leftOuterJoin(moduleMap).map {
            case (module,((n,p,w,_),Some(newModule)))
              => (newModule,(n,p,w))
            case (module,((n,p,w,_),None))
              => (module,(n,p,w))
          }
          .reduceByKey {
            case ( (n1,p1,w1), (n2,p2,w2) ) => (n1+n2,p1+p2,w1+w2)
          }

          // subtract w12 from the sum of w's
          interiWj.filter {
            case ((from,to),w12) => from==to
          }
          .map {
            case ((from,to),w12) => (from,w12)
          }
          .reduceByKey(_+_)
          .rightOuterJoin(sumOnly).map {
            case (module,(Some(w12),(n,p,w)))
            => ( module,( n, p, w-w12,
              Partition.calQ( nodeNumber, n, p, tele, w-w12 )
            ))
            case (module,(None,(n,p,w)))
            => ( module,( n, p, w,
              Partition.calQ( nodeNumber, n, p, tele, w )
            ))
          }
        }

  /***************************************************************************
   * code length calculations
   ***************************************************************************/

        // the sum of q's
        // required in code length calculations
        val qi_sum = newModules.map {
          case (module,(n,p,w,q)) => q
        }
        .sum

        // calculate current code length
        val newCodeLength: Double =
          Partition.calCodeLength(qi_sum,ergodicFreqSum,newModules)

        // if code length is not reduced, terminate
        if( newCodeLength >= partition.codeLength ) {
          logFile.write( "Merging terminates after "
            +(loop-1).toString +" merges" )
          logFile.close
          return partition
        }

  /***************************************************************************
   * logging
   ***************************************************************************/

        // log partitioning
        newPartitioning.saveAsTextFile( outputDir+"/partition_"+loop.toString )

        // log the merge detail
        logFile.write( "Merge " +loop.toString
          +": merging " +partition.modules.count.toString
          +" modules into " +newModules.count.toString +" modules\n"
        )

        // log new code length
        logFile.write( "State " +loop.toString
          +": code length " +newCodeLength.toString +"\n" )

  /***************************************************************************
   * connection properties calculations
   ***************************************************************************/

        // ((module1,module2),wij)
        // map the vertices to new vertices, then aggregate
        val newiWj: RDD[((Int,Int),Double)] = interiWj.filter {
          case ((from,to),weight) => from!=to
        }
        .reduceByKey(_+_)

        // calculate the potential change in code length
        // for each pair of potential module merge
        // for the next loop, where each module seeks
        // to merge with another one greedily
        // ((from,to),deltaL)
        val newDeltaL: RDD[((Int,Int),Double)] = newiWj.map {
          case ((m1,m2),w12) => (m1,(m2,w12))
        }
        .join(newModules).map {
          case (m1,((m2,w12),(n1,p1,w1,q1))) => (m2,(m1,n1,p1,w1,q1,w12))
        }
        .join(newModules).map {
          case (m2,((m1,n1,p1,w1,q1,w12),(n2,p2,w2,q2)))
          => {
            val q12 = Partition.calQ(
              nodeNumber, n1+n2, p1+p2, tele, w1+w2-w12
            )
            ( (m1,m2), InfoFlow.calDeltaL(qi_sum,q1,q2,q12,p1,p2) )
          }
        }

  /***************************************************************************
   * recursive function call
   ***************************************************************************/
        recursiveMerge(
          loop+1,
          Partition(
            nodeNumber,
            tele,
            newPartitioning,
            newiWj,
            newModules,
            newCodeLength
          ),
          newDeltaL
        )
      }

    }
    recursiveMerge( 1, partition, deltaL )
  }
}
