import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.hadoop.mapred.InvalidInputException

object PajekFile
{
  // given lines of declaration of Pajek sections (lines start with '*')
  // return the interval of line numbers, inclusive
  // that belong to those sections
  def intervals( starlines: List[(String,Long)] ): (
    List[(Long,Long)], List[(Long,Long)], List[(Long,Long)]
  ) = {
    // in this block has to use sequential programming
    // since Pajek sectioning is inherently sequential

    var prevline: Long = 1
    var section: String = "Nil"
    var vertexLines: List[(Long,Long)] = Nil
    var edgeLines: List[(Long,Long)] = Nil
    var edgeListLines: List[(Long,Long)] = Nil

    for( (line,index) <- starlines ) {
      section match {
        case "Vertex"   =>
          vertexLines = (prevline,index-1)::vertexLines
        case "Edge"     =>
          edgeLines = (prevline,index-1)::edgeLines
        case "EdgeList" =>
          edgeListLines = (prevline,index-1)::edgeListLines
        case "Nil"      => ()
      }
      prevline = index+1
      val vertexRegex = """(?i)\*Vertices.*?(\d+)""".r
      val edgeRegex = """(?i)\*Arcs""".r
      val edge2Regex = """(?i)\*Edges""".r
      val edgelistRegex = """(?i)\*Arcslist""".r
      val edgelist2Regex = """(?i)\*Edgeslist""".r
      line match {
        case vertexRegex(_*) => section = "Vertex"
        case edgeRegex(_*) => section = "Edge"
        case edge2Regex(_*) => section = "Edge"
        case edgelistRegex(_*) => section ="EdgeList"
        case edgelist2Regex(_*) => section = "EdgeList"
        case _ => section = "Nil"
      }
    }

    if( vertexLines.size != 1 )
      throw new Exception(
        "There must be one and only one vertex number specification"
      )

    ( vertexLines, edgeLines, edgeListLines )
  }

  // check that a line index is within a list of intervals, inclusive
  def withinBound( index: Long, intervals: List[(Long,Long)] ): Boolean = {
    def recursiveFn( index: Long, intervals: List[(Long,Long)] ): Boolean =
      intervals match {
        case Nil => false
        case interval::interval_tail =>
          if( interval._1<=index && index<=interval._2 ) true
          else if( index < interval._1 ) false
          else recursiveFn( index, interval_tail )
      }
    recursiveFn( index, intervals )
  }
}

sealed class PajekFile( sc: SparkContext, val filename: String )
{

  /***************************************************************************
   * Object that reads and stores Pajek net specifications
   * since the nodal weights are not used in the algorithms
   * (only the edge weights are used, for PageRank calculations)
   * the nodal weights will not be read
   ***************************************************************************/
  val(
    n         : Long,                    // number of vertices
    names     : RDD[(Long,String)],      // names of nodes
    sparseMat : RDD[(Long,(Long,Double))] // sparse matrix
  ) = try {

  /***************************************************************************
   * Read raw file, and file aligned with line index
   ***************************************************************************/

    val rawFile = sc.textFile(filename)
    val linedFile = rawFile.zipWithIndex
    linedFile.cache

  /***************************************************************************
   * Grab section declare lines, which begin with a '*'
   * and put into 3 sorted linked lists of type List[(Long,Long)]
   * vertexLines, edgeLines, edgeListLines
   * where each tuple signifies the beginning and ending line index, inclusive
   * this is assuming the number of declare lines are small in the file
   * small meaning <10, probably
   ***************************************************************************/

    val( vertexLines, edgeLines, edgeListLines ): (
      List[(Long,Long)], List[(Long,Long)], List[(Long,Long)]
    ) = {
      val starlines: List[(String,Long)] = {
        val starRegex = """\*([a-zA-Z]+).*""".r
        linedFile.filter {
          case (line,index) => line match {
            case starRegex(id) => true
            case _ => false
          }
        }
      }
      .union( sc.parallelize( Array( ( "", linedFile.count ) ) ) )
      .collect
      .toList
      .sortBy( _._2 )
      PajekFile.intervals( starlines )
    }

  /***************************************************************************
   * Get node number n
   ***************************************************************************/

    val n = {
      val verticesRegex = """(?i)\*Vertices[ \t]+([0-9]+)""".r
      val vertexSpec = linedFile.filter {
        case (line,index) => line match {
          case verticesRegex(_) => true
          case _ => false
        }
      }

      vertexSpec.first._1 match {
        case verticesRegex(number) => number.toLong
      }
    }

  /***************************************************************************
   * declare regex for comments, used to match all lined comments
   ***************************************************************************/

    val commentRegex = """\%(.*)""".r

  /***************************************************************************
   * Read vertex information
   ***************************************************************************/

    val names = {
      val vertexRegex = """[ \t]*?([0-9]+)[ \t]+\"(.*)\".*""".r
      // filter the relevant lines
      val lines = linedFile.filter {
        case (_,index) => PajekFile.withinBound( index, vertexLines )
      }

      val name = lines.map {
        case (line,index) => line match {
          case commentRegex(_*) => ( -1L, "" ) // to be filtered out later
          case vertexRegex(lineindex,vertexname)
            =>( lineindex.toLong, vertexname )
          case _ => throw new Exception(
            "Vertex definition error: line " +index.toString
          )
        }
      }
      // filter out non-positive indices
      .filter {
        case (index,_) => index>0
      }

      // check indices are unique
      name.map {
        case (index,name) => (index,1)
      }
      .reduceByKey(_+_)
      .foreach {
        case (index,count) => if( count > 1 )
          throw new Exception("Vertex index "+index.toString+" is not unique!")
      }

      name
    }

  /***************************************************************************
   * Read edge information and construct connection matrix
   ***************************************************************************/

    val sparseMat = {
      // given the edge specifications (with or without weights)
      // construct a connection matrix
      // if no weight is given, default to weight=1
      // if the same edge is specified more than once, aggregate the weights

      // parse each line that specifies an edge
      val edgeRegex1 =
        """(?i)[ \t]*?([0-9]+)[ \t]+([0-9]*)[ \t]*""".r
      val edgeRegex2 =
        """(?i)[ \t]*?([0-9]+)[ \t]+([0-9]*)[ \t]+([0-9.]+)[ \t]*""".r
      val edge1 =linedFile
      // filter the relevant lines
      .filter {
        case (_,index) => PajekFile.withinBound( index, edgeLines )
      }
      // parse line
      .map {
        case (line,index) => line match {
          case commentRegex(_*) => ( (-1L,-1L), 0.0 ) // to be filtered later
          case edgeRegex1(from,to) =>
            ( (from.toLong,to.toLong), 1.0 )
          case edgeRegex2(from,to,weight) =>
            ( (from.toLong,to.toLong) ,weight.toDouble )
          case _ => throw new Exception(
            "Edge definition error: line " +index.toString
          )
        }
      }

      // parse each line that specifies an edge list
      val edge2 = linedFile
      // filter the relevant lines
      .filter {
        case (_,index) => PajekFile.withinBound( index, edgeListLines )
      }
      // parse line
      .flatMap {
        case (line,index) => line match {
          case commentRegex(_*) =>
            Seq( ((-1L,-1L),0.0) ) // to be filtered later
          case _ => {
            val vertices = line.split("\\s+").filter(x => !x.isEmpty)
            val verticesSlice = vertices.slice(1, vertices.length)
            verticesSlice.map {
              case toVertex => ((vertices(0).toLong, toVertex.toLong), 1.0)
            }
          }
        }
      }

      // combine edge1 +edge2
      edge1.union(edge2)
      // filter commented lines ( (-1,-1) pairs )
      .filter {
        case ((from,to),weight) => !(from==to && to== -1)
      }
      // aggregate the weights
      .reduceByKey(_+_)
      .map {
        case ((from,to),weight) => {
          // check that the vertex indices are valid
          if( from.toLong<1 || from.toLong>n || to.toLong<1 || to.toLong>n )
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
