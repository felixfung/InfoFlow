import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.hadoop.mapred.InvalidInputException

sealed class PajekFile( sc: SparkContext, val filename: String )
{

  /***************************************************************************
   * Object that reads and stores Pajek net specifications
   * since the nodal weights are not used in the algorithms
   * (only the edge weights are used, for PageRank calculations)
   * the nodal weights will not be read
   ***************************************************************************/
  val(
    n         : Int,                    // number of vertices
    names     : RDD[(Int,String)],      // names of nodes
    sparseMat : RDD[(Int,(Int,Double))] // sparse matrix
  ) = try {

  /***************************************************************************
   * Read raw file, and file aligned with line index
   ***************************************************************************/

    val rawFile = sc.textFile(filename)
    val linedFile = rawFile.zipWithIndex

  /***************************************************************************
   * Get node number n
   * and the line number of the vertex specification, vertexLine
   * The latter is to partition the file
   * so that lines vertexLine+1 to vertexLine+n are specifications of vertices
   * and lines before vertexLine and lines after vertexLine+n
   * are specifications of edges
   ***************************************************************************/

    val (n,vertexLine) = {
      val verticesRegex = """(?i)\*Vertices[ \t]+([0-9]+)""".r

      val vertexSpec = linedFile.filter {
        case (line,index) => line match {
          case verticesRegex(_) => true
          case _ => false
        }
      }

      if( vertexSpec.count != 1 )
        throw new Exception(
          "There must be one and only one vertex number specification"
        )

      val n = vertexSpec.first._1 match {
        case verticesRegex(number) => number.toInt
      }
      
      val vertexLine = vertexSpec.first._2.toInt

      ( n, vertexLine )
    }

  /***************************************************************************
   * Read vertex information
   ***************************************************************************/

    val names = {
      // filter the relevant lines
      val vertexLines = linedFile.filter {
        case (_,index) => vertexLine<index && index<=vertexLine+n
      }
      // take away line numbers
      .map {
        case (x,_) => x
      }

      // regex patterns for specifications of vertices
      val vertexRegex =
        """[ \t]*?([0-9]+)[ \t]+\"([0-9a-zA-Z\-]*).*""".r

      vertexLines.map {
        case vertexRegex(index,name) => ( index.toInt, name )
      }
    }

  /***************************************************************************
   * Read edge information and constuct connection matrix
   ***************************************************************************/

    val sparseMat = {

      // filter the relevant lines
      val lineEdges = linedFile.filter {
        case (_,index) => vertexLine > index || index > vertexLine+n+1
      }
      // take away line numbers
      .map {
        case (x,_) => x
      }

      // regex patterns for specifications of edges
      val edgeRegex1 =
        """(?i)[ \t]*?([0-9]+)[ \t]+([0-9]*)[ \t]*""".r
      val edgeRegex2 =
        """(?i)[ \t]*?([0-9]+)[ \t]+([0-9]*)[ \t]+([0-9.]+)[ \t]*""".r

      // given the edge specifications (with or without weights)
      // construct a connection matrix
      // if no weight is given, default to weight=1
      // if the same edge is specified more than once, aggregate the weights
      lineEdges.map {
        case edgeRegex1(from,to) => ( (from.toInt,to.toInt), 1.0 )
        case edgeRegex2(from,to,weight) =>
          ( (from.toInt,to.toInt), weight.toDouble )
        case _ => ((1,1),0.0)
      }
      // aggregate the weights
      .reduceByKey(_+_)
      .map {
        case ((from,to),weight) => {
          // check that the vertex indices are valid
          if( from.toInt<1 || from.toInt>n || to.toInt<1 || to.toInt>n )
            throw new Exception(
              "Edge index must be within 1 and "
                +n.toString+"for connection ("+from.toString+","+to.toString+")"
            )
          // check that the weights are non-negative
          if( weight.toDouble < 0 )
            throw new Exception(
              "Edge weight must be positive for connection ("
                +from.toString+","+to.toString+")"
            )
          (from,(to,weight))
        }
      }
      // weights of zero are legal, but will be filtered out
      .filter {
        case (from,(to,weight)) => weight>0
      }
    }

    (n,names,sparseMat)
  }

  /***************************************************************************
   * Catch exceptions
   ***************************************************************************/
  catch {
    case e: InvalidInputException =>
      throw new Exception("Cannot open file "+filename)
    case e: Exception =>
      throw e
    case _: Throwable =>
      throw new Exception("Error reading file line")
  }
}
