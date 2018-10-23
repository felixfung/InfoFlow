/*****************************************************************************
 * Pajek net file reader
 * file is assumed to be local and read in serially
 *****************************************************************************/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.io.Source
import java.io.FileNotFoundException

import scala.collection.mutable.ListBuffer

object PajekReader
{
  def apply( sc: SparkContext, filename: String ): GraphFrame = {
    try {
      // graph elements stored as local list
      // to be converted to DataFrame and stored in GrapheFrame
      // after file reading
      var vertices = new ListBuffer[(Long,(String,Long))]()
      var edges = new ListBuffer[((Long,Long),Double)]()

      // regexes to match lines in file
      val starRegex = """\*([a-zA-Z]+).*""".r
      val verticesRegex = """(?i)\*Vertices[ \t]+([0-9]+)""".r
      val vertexRegex = """[ \t]*?([0-9]+)[ \t]+\"(.*)\".*""".r
      val edgeRegex1 = """[ \t]*?([0-9]+)[ \t]+([0-9]*)[ \t]*""".r
      val edgeRegex2 = """[ \t]*?([0-9]+)[ \t]+([0-9]*)[ \t]+([0-9.]+)[ \t]*""".r

      // store sectioning of file
      // defaults as "__begin"
      // to give error if the first line in file is not a section declare
      var section: String = "__begin"

      // the number of vertices
      // important since Pajek net format allows nodes to be implicitly declared
      // e.g. when the node number is 6 and only node 1,2,3 are specified,
      // nodes 4,5,6 are still assumed to exist with node name = node index
      var nodeNumber: Long = -1

      var lineNumber = 1 // line number in file, used when printing file error
      // read file serially
      for( line <- Source.fromFile(filename).getLines
        if line.charAt(0) != '%' // skip comments
      ) {
  /***************************************************************************
   * first, check if line begins with '*'
   * which indicates a new section
   * if it is a new section
   * check if it is a vertex section
   * which must be declared once and only once (otherwise throw error)
   * and read in nodeNumber
   ***************************************************************************/

        val newSection = line match {
          // line is section declarator, modify section
          case starRegex(id) => {
            line match {
              case starRegex(expr) => {
                val newSection = expr.toLowerCase
                // check that new section is valid
                if( newSection!="vertices"
                  && newSection!="arcs" && newSection!="arcslist"
                  && newSection!="edges" && newSection!="edgeslist"
                )
                  throw new Exception( "Pajek file format only accepts"
                    +" Vertices, Arcs, Edges, Arcslist, Edgeslist"
                    +" as section declarator: line "+lineNumber )
                // check there is no more than one vertices section
                if( newSection == "vertices" ) {
                  if( nodeNumber != -1 )
                    throw new Exception( "There must be one and only one"
                      +"vertices section" )
                  // read nodeNumber
                  nodeNumber = line match {
                    case verticesRegex(expr) => expr.toLong
                    case _ => throw new Exception( "Cannot read node number:"
                      +" line "+lineNumber.toString )
                  }
                }
                section = "section_def"
                newSection
              }
            }
          }
          // line is not section declarator,
          // section does not change
          case _ => section
        }

  /***************************************************************************
   * Read vertex information
   ***************************************************************************/
        if( section == "vertices" ) {
          val newVertex = line match {
            case vertexRegex( idx, name ) =>
              if( 1<=idx.toLong && idx.toLong<=nodeNumber )
                ( idx.toLong, (name,idx.toLong) )
              // check that index is in valid range
              else throw new Exception(
                "Vertex index must be within [1,"+nodeNumber.toString
                +"]: line " +lineNumber.toString
              )
            // check vertex parsing is correct
            case _ => throw new Exception(
              "Vertex definition error: line " +lineNumber.toString
            )
          }
          vertices += newVertex
        }

  /***************************************************************************
   * Read edge information
   ***************************************************************************/
        else if( section=="edges" || section=="arcs" ) {
          val newEdge = line match {
            case edgeRegex1( src, dst ) =>
              // check that index is in valid range
              if( 1<=src.toLong && src.toLong<=nodeNumber
               && 1<=dst.toLong && dst.toLong<=nodeNumber )
                ( ( src.toLong, dst.toLong ), 1.0 )
              else throw new Exception(
                "Vertex index must be within [1,"+nodeNumber.toString
                +"]: line " +lineNumber.toString
              )
            case edgeRegex2( src, dst, weight ) =>
              // check that index is in valid range
              if( 1<=src.toLong && src.toLong<=nodeNumber
               && 1<=dst.toLong && dst.toLong<=nodeNumber ) {
                // check that weight is not negative
                if( weight.toDouble < 0 ) throw new Exception(
                  "Edge weight must be non-negative: line "+lineNumber.toString
                )
                ( ( src.toLong, dst.toLong ), weight.toDouble )
              }
              else throw new Exception(
                "Vertex index must be within [1,"+nodeNumber.toString
                +"]: line " +lineNumber.toString
              )
            // check vertex parsing is correct
            case _ => throw new Exception(
              "Edge definition error: line " +lineNumber.toString
            )
          }
          edges += newEdge
        }

  /***************************************************************************
   * Read edge list information
   ***************************************************************************/
        else if( section=="edgeslist" || section=="arcslist" ) {
          // obtain a list of vertices
          val vertices = line.split("\\s+").filter(x => !x.isEmpty)
          // obtain a list of edges
          val newEdges = vertices.slice( 1, vertices.length )
          .map {
            case toVertex => ( ( vertices(0).toLong, toVertex.toLong ), 1.0 )
          }
          // append new list to existing list of edges
          edges ++= newEdges
        }

        else if( section != "section_def" )
        {
          throw new Exception("Line does not belong to any sections:"
            +" line "+lineNumber.toString )
        }

  /***************************************************************************
   * prepare for next loop
   ***************************************************************************/
        section = newSection
        lineNumber += 1
      }

  /***************************************************************************
   * check there is at least one vertices section
   ***************************************************************************/
      if( nodeNumber == -1 )
        throw new Exception( "There must be one and only one vertices section" )

  /***************************************************************************
   * check there vertices are unique
   * if vertices are not unique, throw error
   * if a vertex is missing, put in default name
   ***************************************************************************/
      val verticesRDD: RDD[(Long,(String,Long))] = {
        // initiate array
        val verticesArray = new Array[(Long,(String,Long))](nodeNumber.toInt)
        for( idx <- 1 to nodeNumber.toInt )
          verticesArray( idx-1 ) = (-1L,("",-1L))
        // put in each vertices list element to array
        // and check for duplication
        for( (idx,(name,module)) <- vertices ) {
          if( verticesArray(idx.toInt-1)._1 != -1 )
            throw new Exception(
              "Vertex "+verticesArray(idx.toInt-1)._1.toString+" is not unique!"
            )
          verticesArray( idx.toInt-1 ) = ( idx, (name,module) )
        }
        // Pajek file format allows unspecified nodes
        // e.g. when the node number is 6 and only node 1,2,3 are specified,
        // nodes 4,5,6 are still assumed to exist with node name = node index
        for( idx <- 1 to nodeNumber.toInt )
          if( verticesArray( idx-1 )._1 == -1 )
            verticesArray( idx-1 ) = ( idx, (idx.toString,idx) )
        // convert to RDD
        sc.parallelize( verticesArray )
      }

  /***************************************************************************
   * parallelize edges, aggregate edges with the same vertices
   ***************************************************************************/

      val edgesRDD: RDD[(Long,(Long,Double))] = sc.parallelize(edges)
      .reduceByKey(_+_)
      .map {
        case ((from,to),weight) => (from,(to,weight))
      }

  /***************************************************************************
   * return Graph
   ***************************************************************************/

      Graph( verticesRDD, edgesRDD )
    }
    catch {
        case e: FileNotFoundException =>
          throw new Exception("Cannot open file "+filename)
    }
  }
}
