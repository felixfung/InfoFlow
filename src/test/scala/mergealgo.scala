/***************************************************************************
 * merging algorithm unit test case routine
 ***************************************************************************/

import org.scalatest.FunSuite

import org.apache.spark.SparkContext

import scala.io.Source

case class MergeAlgoTest
( val sc: SparkContext, val merge: MergeAlgo)
extends FunSuite
{
  /***************************************************************************
   * function that invokes the InfoFlow algorithm
   * checks the log file for the number of merges and code length
   * and checks the final partitioning scheme
   * so that each test-specific regex pattern matches to a partitioning module
   ***************************************************************************/
  def apply(
    pjFile: String, outputDir: String,
    nMerges: Int,
    codeLengthHi: Double, codeLengthLo: Double,
    partitioningRegex: Array[String]
  ) = {

    // invoke the algorithm
    val pj = new PajekFile( sc, pjFile )
    val nodes = new Nodes( pj, 0.85, 1e-3 )
    val initPartition = Partition.init(nodes)
    val logger = new LogFile(outputDir,true,true,0,false)
    val finalPartition = merge(
      initPartition, logging
    )
    logger.saveText( finalPartition.partitioning, "partition", false )

    // checking the log file
    val logFile = Source.fromFile( outputDir +"/log.txt" )
    val regexL = """State ([0-9]*): code length ([0-9.]*)""".r
    val nanL = """State ([0-9]*): code length NaN""".r
    val regexFinal = """Merging terminates after ([0-9]*) merges""".r
    var prevCodeLength = codeLengthHi +1
    for( line <- logFile.getLines ) {
      line match {
        case nanL(state) => fail("NaN code length")
        case regexL(state,codeLength)
          => {
            if( state == 0 ) {
              if( codeLengthHi-0.05 > codeLength.toDouble
              || codeLength.toDouble > codeLengthHi+0.05 )
                fail("Initial code length incorrect")
            }
            if( state == nMerges ) {
              if( codeLengthLo-0.05 > codeLength.toDouble
              || codeLength.toDouble > codeLengthLo+0.05 )
                fail("Final code length incorrect")
            }
            // assert code length is within designated bound
            if( codeLengthLo > codeLength.toDouble
            || codeLength.toDouble > codeLengthHi )
              fail("Code length incorrect")
            // assert code length is decreasing
            if( codeLength.toDouble >= prevCodeLength )
              fail("Code length did not decrease")
            prevCodeLength = codeLength.toDouble
          }
        case regexFinal(merges) => {
          if( merges.toInt != nMerges )
            fail("Number of merges is incorrect")
        }
        case _ => ()
      }
    }
    logFile.close

  /***************************************************************************
   * check the final partitioning scheme
   ***************************************************************************/

    // each regex should only have one and only one corresponding module
    // which is stored in this array
    var partitioning = partitioningRegex.map {
      case regex => (regex,-1)
    }

    partitioningRegex.foreach {
      case regex => {
        // read partition file and matches regex
        val partFile = Source.fromFile(
          outputDir +"/partition/part-00000"
        )
        val matches = regex.r.findAllIn(partFile.getLines.mkString)
        partFile.close
        // check that each regex corresponds to one and only one module index
        matches.foreach {
          case regex.r(_/*node*/,module) =>
            for( i <- 0 to partitioning.size-1 )
              if( partitioning(i)._1 == regex ) {
                if( partitioning(i)._2 == -1 )
                  partitioning(i) = (regex,module.toInt)
                else if( partitioning(i)._2 != module.toInt )
                  fail("Partitioning is incorrect")
              }
        }
      }
    }
  }
}
