import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

sealed class Nodes
( pajek: PajekFile, val damping: Double, val errTh: Double )
extends Serializable {
  /***************************************************************************
   * Class that stores node information
   ***************************************************************************/
  val n: Int = pajek.n                                       // number of nodes
  val names: RDD[(Int,String)] = pajek.names                      // node names

  val stoMat: Matrix = {                       // sparse matrix for random walk
    val outLinkTotalWeight: RDD[(Int,Double)] = {  // sum of weight of outlinks
      pajek.sparseMat.map {
        case (from,(to,weight)) => (from,weight)
      }
      .reduceByKey(_+_)
    }
    val dangling: RDD[Int] = {                  // nodes without outbound links
      (names leftOuterJoin outLinkTotalWeight).map {
        case (idx,(_,Some(weight))) => -1
        case (idx,(_,None)) => idx
      }
    }
    .filter( _ > -1 )
    val constCol = dangling.map ( // dangling nodes jump to uniform probability
      x => ( x, 1.0/n.toDouble )
    )
    val normMat = (pajek.sparseMat join outLinkTotalWeight).map {
      case (from,((to,weight),totalweight)) =>
        (from,(to, weight/totalweight ))
    }
    new Matrix( normMat, constCol )
  }

  val ergodicFreq: RDD[(Int,Double)] = {            // node ergodic frequencies
    // generate random vector that sums to 1
    //val rGen = new scala.util.Random
    val vRand = names.map{ case (idx,_) => (idx,1.0/n.toDouble) }
                                        // (idx,rGen.nextDouble) }
    val vSum = vRand.values.sum
    val vNorm = vRand.map { case (idx,x) => (idx,x/vSum) }

  /***************************************************************************
   * PageRank algorithm
   ***************************************************************************/
    def pageRank( n: Int, freq: RDD[(Int,Double)] ): RDD[(Int,Double)] = {
      def dist2D( v1: RDD[(Int,Double)], v2: RDD[(Int,Double)] ): Double = {
      // 2D Euclidean distance between two vectors
        val diffSq = (v1 join v2).map {
          case (idx,(e1,e2)) => (e1-e2)*(e1-e2)
        }
        .reduce(_+_)
        Math.sqrt(diffSq)
      }
      // the random walk contribution of the ergodic frequency
      val stoFreq = stoMat * freq
      // the random jump contribution of the ergodic frequency
      val bgFreq = freq.map {
        case (idx,x) => (idx, (1.0-damping)/n.toDouble )
      }
      // combine both random walk and random jump contributions
      val newFreq = (bgFreq leftOuterJoin stoFreq).map {
        case (idx,(bg,Some(sto))) => ( idx, bg+ sto*damping )
        case (idx,(bg,None)) => ( idx, bg )
      }
      // recursive call until freq converges wihtin error threshold
      val err = dist2D(freq,newFreq)
      if( err < errTh ) newFreq
      else pageRank(n,newFreq)
    }
    pageRank(n,vNorm)
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
