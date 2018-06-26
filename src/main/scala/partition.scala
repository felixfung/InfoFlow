import org.apache.spark.rdd.RDD

import java.lang.Math
import java.io._

case class Partition
(
  val nodeNumber: Long,
  val tele: Double,
  val names: RDD[(Long,String)],
  val partitioning: RDD[(Long,Long)],
  val iWj0: RDD[((Long,Long),Double)],
  val iWj:  RDD[((Long,Long),Double)],
  val modules: RDD[(Long,(Long,Double,Double,Double))],
  val codeLength: Double
)
{
  def localCheckpoint: Unit = {
    //names.localCheckpoint
    val dummy = partitioning.count // this action forces RDD evaluation
    partitioning.localCheckpoint
    //iWj0.localCheckpoint
    iWj.localCheckpoint
    modules.localCheckpoint
  }

  /***************************************************************************
   * functions to save graph into json file
   ***************************************************************************/

  // function prints all nodes, with the partition labeling
  def saveJSon( fileName: String ): Unit = {
    // fake nodes to preserve group ordering/coloring
    val fakeNodes = names.map {
      case (idx,_) => (-idx,0L,"",idx)
    }
    .collect
    val nodes = partitioning.join(names).map {
      case (id,(group,name)) => (id,1L,name,group)
    }
    .collect ++fakeNodes
    saveJSon( fileName, nodes.sorted, iWj0.collect.sorted, 0,1 )
  }

  // function prints each partitioning as a node
  def saveReduceJSon( fileName: String ): Unit = {
    val reducedNodes = names.join(partitioning).map {
      case (id,(name,module)) => (module,(name,1L))
    }
    .reduceByKey {
      case ( (name1,count1), (name2,count2) ) =>
        ( name1 +"+"+ name2, count1+count2 )
    }
    .map {
      case (id,(name,count)) => (id,count,name,id)
    }
    saveJSon( fileName,
      reducedNodes.collect.sorted, iWj.collect.sorted, 1,4
    )
  }

  /***************************************************************************
   * inner function to save graph into a json file
   * NOTE: *one* json file, on a local filesystem
   * this assumes a local filesystem can handle the graph data
   * and that a local machine memory can handle the graph data
   ***************************************************************************/
  private def saveJSon(
    fileName: String,
    nodes: Array[(Long,Long,String,Long)], // (id,size,name,group)
    edges: Array[((Long,Long),Double)],   // ((from,to),width)
    minNodeSize: Long, maxNodeSize: Long
  ): Unit = {

    // simple helper function for linear scaling of node and edge size
    def lScale( x0: Double, x1: Double, x2: Double,
                y0: Double, y1: Double
    ): Double
    = if( x2==x0 )
        1
      else
        y0 +(y1-y0) *(x1-x0) /(x2-x0)

    // open file
    val file = new PrintWriter( new File(fileName) )

    // write node data
    if( !nodes.isEmpty ) {
      file.write( "{\n\t\"nodes\": [\n" )
      val nodeCount = nodes.size
      val minSize = nodes.map {
        case (_,size,_,_) => size
      }
      .min
      val maxSize = nodes.map {
        case (_,size,_,_) => size
      }
      .max
      for( idx <- 0 to nodeCount-1 ) {
        nodes(idx) match {
          case (id,size,name,group) => {
            val radius = lScale(
              Math.sqrt(minSize),
              Math.sqrt(size),
              Math.sqrt(maxSize),
            minNodeSize,maxNodeSize)
            file.write(
              "\t\t{\"id\": \"" +id.toString
              +"\", \"size\": \"" +radius.toString
              +"\", \"name\": \"" +name
              +"\", \"group\": \"" +group.toString
              +"\"}"
            )
            if( idx < nodeCount-1 )
              file.write(",")
            file.write("\n")
          }
        }
      }
      file.write( "\t],\n" )
    }

    // write edge data
    if( !edges.isEmpty ) {
      file.write( "\t\"links\": [\n" )
      val edgeCount = edges.size
      val minEdgeSize = edges.map {
        case (_,weight) => weight
      }
      .min
      val maxEdgeSize = edges.map {
        case (_,weight) => weight
      }
      .max
      for( idx <- 0 to edgeCount-1 ) {
        edges(idx) match {
          case ((from,to),weight) =>
            val width = lScale(
              Math.sqrt(minEdgeSize),
              Math.sqrt(weight),
              Math.sqrt(maxEdgeSize),
            1,4)
            file.write(
              "\t\t{\"source\": \"" +from.toString
              +"\", \"target\": \"" +to.toString
              +"\", \"width\": "+width.toString
              +"}"
            )
            if( idx < edgeCount-1 )
              file.write(",")
            file.write("\n")
        }
      }
      file.write("\t]")
    }

    // close file
    file.write( "\n}" )
    file.close
  }

}

object Partition {
  def init( nodes: Nodes ): Partition = {
  /***************************************************************************
   * static function to construct partition given raw nodes
   * initialize with one module per node
   ***************************************************************************/

    // total number of nodes
    val nodeNumber = nodes.n
    // conversion between constant convention
    val tele = 1 -nodes.damping

    // for each node, which module it belongs to
    // here, each node is assigned its own module
    val partitioning: RDD[(Long,Long)] = nodes.names.map {
      case (idx,name) => (idx,idx)
    }

    // probability of transitioning within two modules w/o teleporting
    // the merging operation is symmetric towards the two modules
    // identify the merge operation by
    // (smaller module index,bigger module index)
    val iWj: RDD[((Long,Long),Double)] = {
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

    // the graph edges with named vertex, for graph printing purpose
    val edges = iWj.map {
      case ((from,to),weight) => (from,(to,weight))
    }
    .join(nodes.names)
    .map {
      case (_,((to,weight),fromName)) => (to,(fromName,weight))
    }
    .join(nodes.names)
    .map {
      case (_,((fromName,weight),toName)) => ((fromName,toName),weight)
    }

    // probability of exiting a module without teleporting
    // module information (module #, (n, p, w, q))
    val modules: RDD[(Long,(Long,Double,Double,Double))] = {
      val wi: RDD[(Long,Double)] = {
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
    Partition( nodeNumber, tele, nodes.names, partitioning,
      iWj, iWj, modules, codeLength )
  }

  /***************************************************************************
   * math function for calculating q and L
   ***************************************************************************/

  def calQ( nodeNumber: Long, n: Long, p: Double, tele: Double, w: Double ) =
    tele*(nodeNumber-n)/(nodeNumber-1)*p +(1-tele)*w

  def calDeltaL(
    nodeNumber: Long,
    n1: Long, n2: Long, p1: Double, p2: Double,
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
    modules: RDD[(Long,(Long,Double,Double,Double))]
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
