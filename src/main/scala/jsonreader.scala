/*****************************************************************************
 * class to read Json file
 * using Scala standard library backend
 * return value is always a custom class, JsonObj
 * and up to user to convert
 *
 * usage:
 *   val jsonReader = new JsonReader(filename)
 *   val value = jsonReader.getObj(["key"]).value.toString
 *   val nestedValue = jsonReader.getObj(["object key","key"]).value.toDouble
 *   val moreNesting = jsonReader.getObj
 *                       (["more nesting","object key","key"]).value.toBoolean
 * and so on for final values
 * nested objects can be accessed similarly:
 *   val obj = jsonReader.getObj(["object key"])
 *   val nestedValue = obj.getObj(["key"])
 *   val actualValue = nestedValue.value.toDouble
 * and similarly for higher nested objects
 *
 *****************************************************************************/

import scala.util.parsing.json._

sealed class JsonReader( filename: String )
{
/*****************************************************************************
 * strategy is to read in whole file
 * then wrap it into a JsonObj, which can then be accessed
 *****************************************************************************/
  sealed case class JsonObj( value: Any ) {
    def getObj( keys: Seq[String] ): JsonObj = {
      try {
        keys match {
          case key::Nil =>
            JsonObj( value.asInstanceOf[Map[String,Any]] (key) )
          case key :: nextkeys => {
            val nextObj = JsonObj(
              value.asInstanceOf[Map[String,Any]] (key) )
            nextObj.getObj(nextkeys)
          }
	    }
	  }
      catch {
        case e: java.lang.ClassCastException =>  throw e
        case e: Throwable => throw(e)
      }
    }
  }

/*****************************************************************************
 * strategy is to read in whole file
 * then wrap it into a JsonObj, which can then be accessed
 *****************************************************************************/
  // read in whole file
  val wholeFile: String = {
    val source = scala.io.Source.fromFile(filename)
    try source.mkString
    finally source.close
  }

  // check Json file validity
  {
    val tryParse = JSON.parseFull(wholeFile)
    tryParse match {
      case Some(_) => {}
      case None => throw new Exception("Invalid Json file")
    }
  }

  private val jsonObj = JsonObj( JSON.parseFull(wholeFile).get )
  def getObj( keys: Seq[String] ): JsonObj = jsonObj.getObj(keys)
}
