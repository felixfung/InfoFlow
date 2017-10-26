import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.commons.math.util.MathUtils

import java.io._

object InfoMap
{
  /***************************************************************************
   * given all relevant quantities, calculate new table entry
   ***************************************************************************/
  def tableEntry( nodeNumber: Int, tele: Double,
    qi_sum: Double, n1: Int, n2: Int, p1: Double, p2: Double,
    w1: Double, w2: Double, w12: Double, q1: Double, q2: Double ):
  (Int,Int,Double,Double,Double,Double,Double,Double,Double,Double) = {
    val q12 = Partition.calQ( nodeNumber, n1+n2, p1+p2, tele, w1+w2-w12 )
    if( q12 > 0 ) {
      val (deltaLi12,_) = Partition.calDeltaL(
        nodeNumber,
        n1, n2, p1, p2,
        tele, w1+w2-w12,
        qi_sum, q1, q2
      )
      (n1,n2,p1,p2,w1,w2,w12,q1,q2,deltaLi12)
    }
    else { // q12==0 iff we are merging the entire graph into one module
      (n1,n2,p1,p2,w1,w2,w12,q1,q2,0.0)
    }
  }
  /***************************************************************************
   * calculate deltaL, given deltaLi and q1, q2, q12
   ***************************************************************************/
  def calDeltaL( deltaLi: Double, qi_sum: Double,
  q1: Double, q2: Double, q12: Double ) = {
    val delta_q = q12-q1-q2
    if( qi_sum>0 && qi_sum+delta_q>0 )
      deltaLi +Partition.plogp( qi_sum +delta_q ) -Partition.plogp(qi_sum)
    else
      0
  }
}

class InfoMap extends MergeAlgo
{
  /***************************************************************************
   * ORIGINAL INFOMAP ALGORITHM
   * the function to partition of nodes into modules based on
   * greedily merging the pair of modules that gives
   * the greatest code length reduction
   * until code length is minimized
   ***************************************************************************/
  def apply( partition: Partition, logFile: LogFile ): Partition = {

  /***************************************************************************
   * initial condition
   * grab values from partition, and generate modular connection table
   ***************************************************************************/
    val nodeNumber = partition.nodeNumber
    val tele = partition.tele

    // the code length if all nodes in the graph merge into one module
    // after the nodes are merged, this information is lost
    // so this one time calculation is done in the beginning here
    val ergodicFreqSum = partition.modules.map {
      case (_,(_,p,_,_)) => Partition.plogp(p)
    }
    .sum

    // sum of q's
    // used for deltaL calculations
    val qi_sum = partition.modules.map {
      case (_,(_,_,_,q)) => q
    }
    .sum

    // the table that contains all properties of all connections
    // format of each entry is
    // (
    //   (index1,index2),
    //   (n1,n2,p1,p2,w1,w2,w12,q1,q2,DeltaL12)
    // )
    // with this table, the modular properties are stored redundantly
    // but means that no table joining is required in the loop
    val table = partition.iWj.map {
      case ((m1,m2),w12) => (m1,(m2,w12))
    }
    .join(partition.modules).map {
      case (m1,((m2,w12),(n1,p1,w1,q1))) => (m2,(m1,n1,p1,w1,q1,w12))
    }
    .join(partition.modules).map {
      case (m2,((m1,n1,p1,w1,q1,w12),(n2,p2,w2,q2))) =>
        (
          (m1,m2),
          InfoMap.tableEntry(nodeNumber,tele,qi_sum,n1,n2,p1,p2,w1,w2,w12,q1,q2)
        )
    }

  /***************************************************************************
   * recursive function
   * meat of algorithm
   * greedily merge modules until code length is minimized
   * returns ( final minimized code length, node partitioning )
   * node partitioning in the form of RDD[(node index, module index)]
   ***************************************************************************/
    def recursiveMerge(
      loop: Int,
      codeLength: Double,
      qi_sum: Double,
      partitioning: RDD[(String,Int)],
      table: RDD[((Int,Int),(Int,Int,
        Double,Double,Double,Double,Double,Double,Double,Double))]
    ): Partition = {

  /***************************************************************************
   * output code length and partitioning to files
   ***************************************************************************/
      logFile.save( partitioning, "/partition_"+(loop-1).toString, true )
      logFile.write(
        "State " +(loop-1).toString
        +": code length " +codeLength.toString +"\n",
        false
      )

  /***************************************************************************
   * loop termination routine
   ***************************************************************************/
      def terminate( nodeNumber: Int, tele: Double,
      partitioning: RDD[(String,Int)],
      table: RDD[((Int,Int),(Int,Int,
      Double,Double,Double,Double,Double,Double,Double,Double))],
      codeLength: Double ) = {
        logFile.write( "Merging terminates after " +(loop-1).toString +" merges", false )
        logFile.close
        val iWj = table.map {
          case ((from,to),(_,_,_,_,_,_,w12,_,_,_))
          => ((from,to),w12)
        }
        // the modular properties cannot be fully recovered from the table
        // since table only stores modular properties
        // when they are associated with an edge
        // so, we return an empty modules RDD
        val modules = table.map {
          case _ => (0,(0,0.0,0.0,0.0))
        }
        .filter {
          case _ => false
        }
        Partition( nodeNumber, tele, partitioning,
          iWj, modules, codeLength )
      }

  /***************************************************************************
   * if there are no modules to merge,
   * teminate
   ***************************************************************************/
      if( table.count == 0 )
      {
        terminate( nodeNumber, tele,
          partitioning, table, codeLength )
      }
  /***************************************************************************
   * find pair to merge according to greatest reduction in code length
   * and grab all associated quantities
   ***************************************************************************/
      else {
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

  /***************************************************************************
   * CALCULATE NEW SCALAR PROPERTIES
   * these relate to the newly merged modules
   ***************************************************************************/

        // calculate new modular properties
        val n12 = n1 +n2
        val p12 = p1 +p2
        val w12 = w1 +w2 -w102
        val q12 = Partition.calQ( nodeNumber, n12, p12, tele, w12 )
        // calculate new qi_sum
        val new_qi_sum = qi_sum +q12 -q1 -q2

        // calculate new code length
        val deltaL = InfoMap.calDeltaL( deltaLi12, qi_sum, q1, q2, q12)
        val newCodeLength = codeLength +deltaL

  /***************************************************************************
   * if the code length cannot be decreased, then terminate
   * otherwise recalculate and recurse
   ***************************************************************************/

        // deltaL12==0 iff the entire graph is merged into one module
        // if that is the selected merge, that means all other merges is +ve
        // then we terminate
        if( deltaL == 0 )
        {
          if( codeLength < -ergodicFreqSum )
            terminate( nodeNumber, tele,
              partitioning, table, codeLength )
          else {
            val newPartitioning = partition.partitioning.map {
              case (node,module) => (node,1)
            }
            terminate( nodeNumber, tele,
              newPartitioning, table, -ergodicFreqSum )
          }
        }
        // if code length cannot be decreased then terminate recursive algorithm
        else if( deltaL > 0 )
        {
          terminate( nodeNumber, tele,
            partitioning, table, codeLength )
        }
        else {
          // log merging details
          logFile.write( "Merge " +loop.toString +": merging modules "
            +merge1.toString +" and " +merge2.toString
            +" with code length reduction " +deltaL.toString +"\n", false )
          // register partition to lower merge index
          val newPartitioning = partitioning.map {
            case (node,module) =>
              if( module==merge2 ) (node,merge1) else (node,module)
          }

  /***************************************************************************
   * UPDATE TABLE
   * if it is an intra-modular connection (both indices match), delete
   * if it relates to the newly merged module (one index match),
   *     recalculate w_ij, q_ij, deltaL_ij
   * if it is unrelated, (no index match), leave alone
   ***************************************************************************/

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

  /***************************************************************************
   * recursive call
   ***************************************************************************/

          recursiveMerge( loop+1, newCodeLength, new_qi_sum,
            newPartitioning, newTable )
        }
      }
    }

  /***************************************************************************
   * main function invokes recursive function
   ***************************************************************************/
    val newPartition =
      recursiveMerge( 1,
        partition.codeLength,
        qi_sum,
        partition.partitioning,
        table
      )

    newPartition
  }

}
