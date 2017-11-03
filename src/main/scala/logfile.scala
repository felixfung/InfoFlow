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

class LogFile( val outputDir: String, val debugging: Boolean ) {
    /** *************************************************************************
      * Constructor: create directory and log file within
      * **************************************************************************/

    // create output directory
    new File(outputDir).mkdirs
    // create file to store the loop of code lengths
    val logFile = new PrintWriter(new File(outputDir + "/log.txt"))

    /** *************************************************************************
      * write message to log file
      * **************************************************************************/
    def write(msg: String, dbgMsg: Boolean) =
      if (!dbgMsg || debugging)
        logFile.write(msg)

    /** *************************************************************************
      * save an RDD object to a text file
      * **************************************************************************/
    def save[T](rdd: RDD[T], id: String, dbgMsg: Boolean) =
      if (!dbgMsg || debugging)
        rdd.saveAsTextFile(outputDir + "/" + id)

    /** *************************************************************************
      * close log file
      * **************************************************************************/
    def close = logFile.close
}
