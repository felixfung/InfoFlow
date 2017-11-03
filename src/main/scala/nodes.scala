import org.apache.spark.rdd.RDD

sealed class Nodes
( pajek: PajekFile, val damping: Double, val errTh: Double )
extends Serializable {
  /***************************************************************************
   * Class that stores nodal information
   ***************************************************************************/

  /***************************************************************************
   * Grab these directly from the PajekFile
   ***************************************************************************/
  val n: Int = pajek.n
  val names: RDD[(Int,String)] = pajek.names

  /***************************************************************************
   * Normalized sparse matrix for PageRank calculation
   ***************************************************************************/
  val stoMat: Matrix = {

    // sum of weight of outlinks
    val outLinkTotalWeight: RDD[(Int,Double)] = {
      pajek.sparseMat.map {
        case (from,(to,weight)) => (from,weight)
      }
      .reduceByKey(_+_)
    }

    // nodes without outbound links are "dangling"
    val dangling: RDD[Int] = names.leftOuterJoin(outLinkTotalWeight)
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
    val normMat = pajek.sparseMat.join(outLinkTotalWeight)
    .map {
      case (from,((to,weight),totalweight)) =>
        (from,(to, weight/totalweight ))
    }

    new Matrix( normMat, constCol )
  }

  /***************************************************************************
   * Ergodic nodal frequencies
   ***************************************************************************/
  val ergodicFreq: RDD[(Int,Double)] = {
    val vRand = names.map{ case (idx,_) => (idx,1.0/n.toDouble) }
    val vSum = vRand.values.sum
    val vNorm = vRand.map { case (idx,x) => (idx,x/vSum) }
    Nodes.pageRank( stoMat, vNorm, n, damping, errTh )
  }
}

object Nodes {
  /***************************************************************************
   * PageRank algorithm
   ***************************************************************************/
  def pageRank( stoMat: Matrix, freq: RDD[(Int,Double)],
    n: Int, damping: Double, errTh: Double )
  : RDD[(Int,Double)] = {

    // 2D Euclidean distance between two vectors
    def dist2D( v1: RDD[(Int,Double)], v2: RDD[(Int,Double)] ): Double = {
      val diffSq = (v1 join v2).map {
        case (idx,(e1,e2)) => (e1-e2)*(e1-e2)
      }
      .sum
      Math.sqrt(diffSq)
    }

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
    else pageRank( stoMat, newFreq, n, damping, errTh )
  }
}

sealed case class Matrix
( val sparse: RDD[(Int,(Int,Double))],
  val constCol: RDD[(Int,Double)] )
extends Serializable {
  /***************************************************************************
   * A matrix class stored using sparse entries
   ***************************************************************************/
  def *( vector: RDD[(Int,Double)] ): RDD[(Int,Double)] = {

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
