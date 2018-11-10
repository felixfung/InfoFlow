/***************************************************************************
 * Test Suite for Partition
 * calculates Partition numerics and checks for consistency
 * with previous calculated results
 ***************************************************************************/

import org.scalactic.TolerantNumerics

class PartitionTest extends SparkTestSuite
{
    implicit val doubleEquality =
      TolerantNumerics.tolerantDoubleEquality(1e-1)

  test("Single node partitioning") {
    val vertices = sc.parallelize( List( (1L,("1",1L)) ) )
    val edges = sc.parallelize( List[(Long,(Long,Double))]() )
    val graph0 = Graph( vertices, edges )
    val partition = Partition.init( graph0, 0.15 )
    assert(modulesEq(
      partition.vertices.collect,
      Array( (1,(1,1.0,0.0,0.0)) )
    ))
    assert( partition.codelength == 0 )
  }

  test("Two-node partitioning") {
    val vertices = sc.parallelize( List( (1L,("1",1L)), (2L,("2",2L)) ) )
    val edges = sc.parallelize( List( (1L,(2L,1.0)), (2L,(1L,1.0)) ) )
    val graph0 = Graph( vertices, edges )
    val partition = Partition.init( graph0, 0.15 )
    assert(modulesEq(
      partition.vertices.collect,
      Array( (1,(1,0.5,0.5,0.5)), (2,(1,0.5,0.5,0.5)) )
    ))
    assert(edgesEq(
      partition.edges.collect,
      Array( (1,(2,.5)), (2,(1,.5)) )
    ))
    assert( partition.codelength == 3 )
  }

  test("Trivial partitioning with self loop should not change result") {
    val vertices = sc.parallelize(
      List[(Long,(String,Long))]( (1,("1",1)), (2,("2",2)) ) )
    val edges = sc.parallelize(
      List[(Long,(Long,Double))]( (1,(2,1)), (2,(1,1)), (1,(1,1)) ) )
    val graph0 = Graph( vertices, edges )
    val partition = Partition.init( graph0, 0.15 )
    assert(modulesEq(
      partition.vertices.collect,
      Array( (1L,(1L,0.5,0.5,0.5)), (2L,(1L,0.5,0.5,0.5)) )
    ))
    assert(edgesEq(
      partition.edges.collect,
      Array( (1L,(2L,.5)), (2L,(1L,.5)) )
    ))
    assert( partition.codelength == 3 )
  }

  test("Non-trivial graph") {
    val vertices = sc.parallelize( List[(Long,(String,Long))](
      (1,("1",1)), (2,("2",2)), (3,("3",3)), (4,("4",4))
    ))
    val edges = sc.parallelize( List[(Long,(Long,Double))](
      (1,(2,1)), (2,(3,1)), (1,(3,1)), (3,(1,1)), (4,(3,1))
    ))
    val graph0 = Graph( vertices, edges )
    val partition = Partition.init( graph0, 0.15 )
    assert(modulesEq(
      partition.vertices.collect,
      Array(
        ( 1L, ( 1L, 0.3725, 0.3725, 0.3725 )),
        ( 2L, ( 1L, 0.195, 0.195, 0.195 )),
        ( 3L, ( 1L, 0.395, 0.395, 0.395 )),
        ( 4L, ( 1L, 0.0375, 0.0375, 0.0375 ))
      )
    ))
    assert(edgesEq(
      partition.edges.collect,
      Array(
        ( 1L, ( 2L, 0.5 *0.3725 )),
        ( 1L, ( 3L, 0.5 *0.3725 )),
        ( 2L, ( 3L, 1 *0.195 )),
        ( 3L, ( 1L, 1 *0.395 )),
        ( 4L, ( 3L, 1 *0.0375 ))
      )
    ))
    assert( Math.abs( partition.codelength -3.70 ) < 0.01 )
  }

  test("Non-trivial graph codelength calculation after merging modules 2, 3") {
    // this test checks deltaL calculation,
    // and compare that with codelength before and after merging

    // initial graph
    val vertices0 = Array[(Long,(Long,Double,Double,Double))](
      ( 1L, ( 1L, 0.3725, 0.3725, 0.3725 )),
      ( 2L, ( 1L, 0.195, 0.195, 0.195 )),
      ( 3L, ( 1L, 0.395, 0.395, 0.395 )),
      ( 4L, ( 1L, 0.0375, 0.0375, 0.0375 ))
    )
    val edges0 = Array[(Long,(Long,Double))](
      (1,(2,1)), (2,(3,1)), (1,(3,1)), (3,(1,1)), (4,(3,1))
    )
    val probSum = vertices0.map {
      case (_,(_,p,_,_)) => CommunityDetection.plogp(p)
    }
      .sum

    // "dummy" Partition object, only nodeNumber and tele are needed
    // for CommunityDetection.calDeltaL()
    val netDummy = Partition( 4, 0.15,
      sc.parallelize(vertices0), sc.parallelize(edges0), 0, 0 )
    val codelength0 = 3.70 // calculated in previous test

    // dL when modules 2, 3 are merged
    val dL = CommunityDetection.calDeltaL(
      netDummy, 1, 1, 0.195, 0.395, 0.195+0.395-0.195, 1, 0.195, 0.395 )

    // vertices when modules 2, 3 are merged
    val vertices1 = sc.parallelize( Array[(Long,(Long,Double,Double,Double))](
      ( 1L, ( 1L, 0.3725, 0.3725, 0.3725 )),
      ( 2L, ( 2L, 0.195+0.395, 0.195+0.395-0.195,
        CommunityDetection.calQ(4,2,0.195+0.395,0.15,0.195+0.395-0.195) )),
      ( 4L, ( 1L, 0.0375, 0.0375, 0.0375 ))
    ))

    // calculate new codelength and compare
    val codelength1 = CommunityDetection.calCodelength( vertices1, probSum )
    assert( Math.abs( codelength0+dL -codelength1 ) < 0.01 )
  }

  /***************************************************************************
   * this test suite is mostly testing for numerical calculation correctness
   * hence, here define floating point equality within Row(...)
   ***************************************************************************/
  def modulesEq(
    array1: Array[(Long,(Long,Double,Double,Double))],
    array2: Array[(Long,(Long,Double,Double,Double))]
  ): Boolean = {
    var equality: Boolean = true
    if( array1.length != array2.length )
      equality = false
    val arrayA = array1.sortBy(_._1)
    val arrayB = array2.sortBy(_._1)
    for( i <- 0 to arrayA.length-1 if equality ) {
      equality = arrayA(i)._1 == arrayB(i)._1
      if( equality ) {
        equality = arrayA(i)._2 match {
          case (n1,p1,w1,q1) => arrayB(i)._2 match {
            case (n2,p2,w2,q2) =>
              n1==n2 && Math.abs(p1-p2)<0.01 &&
              Math.abs(w1-w2)<0.01 && Math.abs(q1-q2)<0.01
          }
        }
      }
      equality
    }
    equality
  }
  def edgesEq(
    array1: Array[(Long,(Long,Double))],
    array2: Array[(Long,(Long,Double))]
  ): Boolean = {
    var equality: Boolean = true
    if( array1.length != array2.length )
      equality = false
    val arrayA = array1.sortBy(_._1)
    val arrayB = array2.sortBy(_._1)
    for( i <- 0 to arrayA.length-1 if equality ) {
      equality = arrayA(i)._1 == arrayB(i)._1
      if( equality ) {
        equality = arrayA(i)._2 match {
          case (_,w1) => arrayB(i)._2 match {
            case (_,w2) => Math.abs(w1-w2) < 0.01
          }
        }
      }
      equality
    }
    equality
  }
}
