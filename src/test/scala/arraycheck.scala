class ArrayCheck( a1: Array[(Int,Double)], a2: Array[(Int,Double)] )
{
  val v1 = a1.sorted
  val v2 = a2.sorted
  val msg = printString(v1) +" does not equal "+ printString(v2)
  def printString( array: Array[(Int,Double)] ): String = {
    var res = "Array( "
    array.foreach{
      case (idx,entry)
        => res += "("+idx.toString+","+entry.toString+") "
    }
    res +")"
  }
  val eq: Boolean = {
    var failed = false
    if( a1.length != a2.length )
      failed = true
    else for( i <- 0 to a1.length-1 ) {
      v1(i) match {
        case (idx1,e1) => v2(i) match {
          case (idx2,e2) =>
            if( idx1!=idx2 || Math.abs(e1-e2) >=1e-3 )
              failed = true
        }
      }
    }
    !failed
  }
}
