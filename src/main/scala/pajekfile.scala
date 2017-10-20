import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.hadoop.mapred.InvalidInputException

sealed class PajekFile( sc: SparkContext, val filename: String ) {
  val(
    n         : Int,                    // number of vertices
    names     : RDD[(Int,String)],      // names of nodes
    weights   : RDD[(Int,Double)],      // weights of nodes
    sparseMat : RDD[(Int,(Int,Double))] // sparse matrix
  ) = try {
  /***************************************************************************
   * Read raw file and get n
   ***************************************************************************/
    val rawFile = sc.textFile(filename)
    val verticesRegex = """\*Vertices[ \t]+([0-9]+)""".r
    val parseVerticesWholeFile = rawFile.zipWithIndex().map {
      case (line,index) => line match {
        case verticesRegex(vertices) => (vertices.toInt,index.toInt)
        case _ => (0,-1)
      }
    }
    val parseVertices = parseVerticesWholeFile
      .filter{ case (x,index)=>index>=0 }.collect
    if( parseVertices.size != 1 )
      throw new Exception(
        "There must be one and only one vertex number specification"
      )
    val n = parseVertices(0)._1.toInt
  /***************************************************************************
   * Read vertex information
   ***************************************************************************/
    val verticesStart = parseVertices(0)._2.toInt
    val verticesEnd = verticesStart +n+1
    val lineVertices = rawFile.zipWithIndex().filter{
      case (x,index) => verticesStart<index && index<verticesEnd
    }.map { case (x,index) => x}
    val vertexRegex1 =
      """[ \t]*?([0-9]+)[ \t]+\"([0-9a-zA-Z\-]*)\"[ \t]+([0-9.]+)[ \t]*""".r
    val vertexRegex2 =
      """[ \t]*?([0-9]+)[ \t]+\"([0-9a-zA-Z\-]*)\"[ \t]*""".r
    val weights = lineVertices.map {
      case vertexRegex1(index,name,weight) => {
        if( index.toInt<0 || index.toInt>n )
          throw new Exception(
            "Vertex index must be within 1 and "
              +n.toString+"for index "+index.toString
          )
        if( weight.toDouble <= 0 )
          throw new Exception(
            "Vertex weight must be positive for index "+index.toString
          )
        (index.toInt,weight.toDouble)
      }
      case vertexRegex2(index,name) => {
        (index.toInt,1D)
      }
    }
    val names = lineVertices.map {
      case vertexRegex1(index,name,weight) => (index.toInt,name)
      case vertexRegex2(index,name) => (index.toInt,name)
    }
  /***************************************************************************
   * Read edge information and constuct connection matrix
   ***************************************************************************/
    val lineEdges = rawFile.zipWithIndex().filter{
      case (x,index) => index > verticesEnd
    }
    .map { case (x,index) => x}
    val edgeRegex2 =
      """[ \t]*?([0-9]+)[ \t]+([0-9]*)[ \t]+([0-9.]+)[ \t]*""".r
    val edgeRegex1 =
      """[ \t]*?([0-9]+)[ \t]+([0-9]*)[ \t]*""".r

    val sparseMat = lineEdges.map {
      case edgeRegex1(from,to) => {
        if( from.toInt<0 || from.toInt>n )
          throw new Exception(
            "Edge index must be within 1 and "
              +from.toString+"for connection ("+from.toString+","+to.toString+")"
          )
        if( to.toInt<0 || to.toInt>n )
          throw new Exception(
            "Edge index must be within 1 and "
              +to.toString+"for connection ("+from.toString+","+to.toString+")"
          )
        ( (from.toInt,to.toInt), 1.toDouble )
      }
      case edgeRegex2(from,to,weight)
        => ( (from.toInt,to.toInt), weight.toDouble )
    }
    .reduceByKey(_+_)
    .map {
      case ( (from,to), weight ) => {
        if( from.toInt<0 || from.toInt>n )
          throw new Exception(
            "Edge index must be within 1 and "
              +from.toString+" for connection ("+from.toString+","+to.toString+")"
          )
        if( to.toInt<0 || to.toInt>n )
          throw new Exception(
            "Edge index must be within 1 and "
              +to.toString+" for connection ("+from.toString+","+to.toString+")"
          )
        if( weight.toDouble < 0 )
          throw new Exception(
            "Edge weight must be positive for connection ("
              +from.toString+","+to.toString+")"
          )
        (from,(to,weight))
      }
    }
    .filter {
      case (from,(to,weight)) => weight>0
    }
    (n,names,weights,sparseMat)
  }
  catch {
    case e: InvalidInputException =>
      throw new Exception("Cannot open file "+filename)
    case e: Exception =>
      throw e
    case _: Throwable =>
      throw new Exception("Error reading file line")
  }
}
