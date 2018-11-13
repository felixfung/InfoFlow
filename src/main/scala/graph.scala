/*****************************************************************************
 * case class to store network data
 *****************************************************************************/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

sealed case class Graph
(
  vertices: RDD[(Long,(String,Long))], // | index , name , module |
  edges: RDD[(Long,(Long,Double))] // | index from , index to , weight |
)
