/*****************************************************************************
 * class that provides unit testing framework for Spark
 * inherit to have Spark initialization and termination handled automatically
 * access Spark via sqlContext
 *****************************************************************************/

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

class SparkTestSuite extends FunSuite with BeforeAndAfter
{
  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
  var sc: SparkContext = _
  before {
    val conf = new SparkConf()
      .setAppName("InfoFlow test suite")
      .setMaster("local[*]")
    sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
  }
  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
  after {
    if( sc != null )
      sc.stop
  }
}
