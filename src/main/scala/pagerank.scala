/*****************************************************************************
 * PageRank calculation
 * given initial ergodic frequency and edges
 * calculation terminates when consequtive iterations differ less than errTh
 *****************************************************************************/

import org.apache.spark.rdd.RDD

object PageRank
{
  def apply(
    edges: Matrix, freq: RDD[(Long,Double)],
    n: Long, damping: Double, errTh: Double, loop: Long
  ): RDD[(Long,Double)] = {

    // 2D Euclidean distance between two vectors
    def dist2D( v1: RDD[(Long,Double)], v2: RDD[(Long,Double)] ): Double = {
      val diffSq = (v1 join v2).map {
        case (idx,(e1,e2)) => (e1-e2)*(e1-e2)
      }
      .sum
      Math.sqrt(diffSq)
    }

    // create local checkpoint to truncate RDD lineage (every ten loops)
    if( loop%10 == 0 )
      freq.localCheckpoint

    // the random walk contribution of the ergodic frequency
    val stoFreq = stoMat *freq

    // the random jump contribution of the ergodic frequency
    val bgFreq = freq.map {
      case (idx,_) => (idx, (1.0-damping)/n.toDouble )
    }

    // combine both random walk and random jump contributions
    val newFreq = (bgFreq leftOuterJoin stoFreq).map {
      case (idx,(bg,Some(sto))) => ( idx, bg+ sto*damping )
      case (idx,(bg,None)) => ( idx, bg )
    }

    // recursive call until freq converges wihtin error threshold
    val err = dist2D(freq,newFreq)
    if( err < errTh ) newFreq
    else pageRank( stoMat, newFreq, n, damping, errTh, loop+1 )
  }
}

  /***************************************************************************
   * A matrix class stored using sparse entries
   * this is used for calculating PageRank
   ***************************************************************************/

sealed case class Matrix
( sparse: RDD[(Long,(Long,Double))],
  constCol: RDD[(Long,Double)] )
extends Serializable {
  def *( vector: RDD[(Long,Double)] ): RDD[(Long,Double)] = {

    // constCol is an optimization,
    // if all entries within a column has constant value
    val constColProd = (constCol join vector).map {
      case (from,(col,vec)) => col*vec
    }
    .sum

    val constColVec = vector.map {
      case (idx,x) => (idx,constColProd)
    }

    val matTimesVec = (sparse join vector).map {
      case (from,((to,matrix),vec)) => (to,vec*matrix)
    }
    .reduceByKey(_+_)

    val matTimesVecPlusConstCol = (matTimesVec rightOuterJoin constColVec)
    .map {
      case (idx,(Some(x),col)) => (idx,x+col)
      case (idx,(None,col)) => (idx,col)
    }

    matTimesVecPlusConstCol
  }
}
