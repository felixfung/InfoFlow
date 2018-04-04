import org.apache.spark.rdd.RDD

import java.io._

  /***************************************************************************
   * Helper class to write log data and save RDDs
   * each object may or may not be for debugging purpose
   * and each operation might or might not be for debugging purpose
   * so that an operation is only performed if:
   *   (1) the operation is not for debugging, OR
   *   (2) the log file object is for debugging
   ***************************************************************************/

class LogFile(
  val outputDir: String,
  val writeLog: Boolean,
  val rddText: Boolean,
  val rddJSon: Int, // 0->no output; 1->output full graph; 2->reduced graph
  val logSteps: Boolean,
  val append: Boolean
) {
  /***************************************************************************
   * Constructor: create directory and log file within
   ***************************************************************************/

  // create output directory
  new File(outputDir).mkdirs
  // create file to store the loop of code lengths
  val logFile = {
    val file = new File( outputDir +"/log.txt" )
    if( append && file.exists && !file.isDirectory )
      new PrintWriter( new FileOutputStream(file,true) )
    else
      new PrintWriter(file)
  }

  /***************************************************************************
   * write message to log file
   ***************************************************************************/
  def write( msg: String ) =
    if( writeLog ) {
      /*if( append )*/ logFile.append(msg)
      //else logFile.write(msg)
    }

  /***************************************************************************
   * save an RDD object to a text file
   ***************************************************************************/
  def saveText[T]( rdd: RDD[T], id: String, stepping: Boolean ) =
    if( rddText && ( !stepping || logSteps ) )
      rdd.saveAsTextFile( outputDir +"/" +id )

  /***************************************************************************
   * save a partition object to a JSON file
   ***************************************************************************/
  def saveJSon( partition: Partition, id: String, stepping: Boolean ) =
    if( rddJSon>0 && ( !stepping || logSteps ) ) {
      if( rddJSon == 1 )
        partition.saveJSon( outputDir +"/" +id )
      else if( rddJSon == 2 ) {
        partition.saveReduceJSon( outputDir +"/" +id )

      }
    }

  /***************************************************************************
   * close log file
   ***************************************************************************/
  def close = logFile.close
}
