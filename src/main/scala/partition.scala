import org.apache.spark.rdd.RDD

import java.lang.Math

case class Partition
(
  val nodeNumber: Int,
  val tele: Double,
  val partitioning: RDD[(String,Int)],
  val iWj: RDD[((Int,Int),Double)],
  val modules: RDD[(Int,(Int,Double,Double,Double))],
  val codeLength: Double
)

object Partition {
  def init( nodes: Nodes ): Partition = {
  /***************************************************************************
   * static function to construct partition given raw nodes
   * initialize with one module per node
   ***************************************************************************/

    // total number of nodes
    val nodeNumber = nodes.names.count
    // conversion between constant convention
    val tele = 1 -nodes.damping

    // for each node, which module it belongs to
    // here, each node is assigned its own module
    val partitioning: RDD[(String,Int)] = nodes.names.map {
      case (idx,name) => (name,idx)
    }

    // probability of transitioning within two modules w/o teleporting
    // the merging operation is symmetric towards the two modules
    // identify the merge operation by
    // (smaller module index,bigger module index)
    val iWj: RDD[((Int,Int),Double)] = {
      // sparse transition matrix without self loop
      val stoMat = nodes.stoMat.sparse.filter {
        case (from,(to,weight)) => from != to
      }
      stoMat.join(nodes.ergodicFreq)
      .map {
        case (from,((to,transition),ergodicFreq))
          => if( from < to )
               ((from,to),ergodicFreq*transition)
             else
               ((to,from),ergodicFreq*transition)
      }
      .reduceByKey(_+_)
    }

    // probability of exiting a module without teleporting
    // module information (module #, (n, p, w, q))
    val modules: RDD[(Int,(Int,Double,Double,Double))] = {
      val wi: RDD[(Int,Double)] = {
        nodes.stoMat.sparse.filter { // filter away self loop
          case (from,(to,_)) => from != to
        }
        .join(nodes.ergodicFreq)
        .map {
          case (from,((_,transition),ergodicFreq))
            => (from,ergodicFreq*transition)
        }
        .reduceByKey(_+_)
      }

      nodes.ergodicFreq.leftOuterJoin(wi)
      .map {
        case (idx,(freq,Some(w)))
          => (idx,(1,freq,w,tele*freq+(1-tele)*w))
        case (idx,(freq,None))
          => (idx,(1,freq,0,tele*freq))
      }
      // since iWj is normalized per "from" node,
      // w and q are mathematically identical to p
      // as long as there is at least one connection
      /*nodes.ergodicFreq.map {
        case (idx,freq) => (idx,(1,freq,freq,freq))
      }*/
    }

    // calculate current code length
    val codeLength: Double = {
      val qi_sum = modules.map {
        case (_,(_,_,_,q)) => q
      }
      .sum
      val ergodicFreqSum = modules.map {
        case (_,(_,p,_,_)) => plogp(p)
      }
      .sum
      calCodeLength( qi_sum, ergodicFreqSum, modules )
    }

    // construct partition
    Partition( nodeNumber.toInt, tele, partitioning,
      iWj, modules, codeLength )
  }

  /***************************************************************************
   * math function for calculating q and L
   ***************************************************************************/

  def calQ( nodeNumber: Int, n: Int, p: Double, tele: Double, w: Double ) =
    tele*(nodeNumber-n)/(nodeNumber-1)*p +(1-tele)*w

  def calDeltaL(
    nodeNumber: Int,
    n1: Int, n2: Int, p1: Double, p2: Double,
    tele: Double, w12: Double,
    qi_sum: Double, q1: Double, q2: Double
  ) = {
    val q12 = calQ( nodeNumber, n1+n2, p1+p2, tele, w12 )
    val delta_q = q12-q1-q2
    val deltaLi = (
      -2*plogp(q12) +2*plogp(q1) +2*plogp(q2)
      +plogp(p1+p2+q12) -plogp(p1+q1) -plogp(p2+q2)
    )
    val deltaL =
      if( qi_sum>0 && qi_sum+delta_q>0 )
        deltaLi +Partition.plogp( qi_sum +delta_q ) -Partition.plogp(qi_sum)
      else
        0
    ( deltaLi, deltaL )
  }

  def calCodeLength(
    qi_sum: Double, ergodicFreqSum: Double,
    modules: RDD[(Int,(Int,Double,Double,Double))]
  ) =
    if( modules.count > 1 ) (
      modules.map {
        case (_,(_,p,_,q)) =>
          -2*Partition.plogp(q) +Partition.plogp(p+q)
      }
      .sum +Partition.plogp(qi_sum) -ergodicFreqSum
    )
    else
    // if the entire graph is merged into one module,
    // there is easy calculation
      -ergodicFreqSum

  /***************************************************************************
   * math function of plogp(x) for calculation of code length
   ***************************************************************************/
  def log( double: Double ) = Math.log(double)/Math.log(2.0)
  def plogp( double: Double ) = double*log(double)
}
