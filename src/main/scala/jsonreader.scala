  /***************************************************************************
   * class to read Json file
   * using Scala standard library backend
   * return value is always Any
   * and up to user to convert
   *
   * usage:
   *   val jsonReader = new JsonReader(filename)
   *   val value = jsonReader.getVal("key")
   *   val nestedValue = jsonReader.getVal("object key","key")
   *   val moreNesting = jsonReader.getVal("more nesting","object key","key")
   *   and so on
   *
   * if requesting an object rather than a value
   * then the returned value is a mapping (if user cast from Any)
   ***************************************************************************/

import scala.util.parsing.json._

sealed class JsonReader( filename: String )
{
  val wholeFile: String = {
    val source = scala.io.Source.fromFile(filename)
    try source.mkString
    finally source.close
  }

  {
    val tryParse = JSON.parseFull(wholeFile)
    tryParse match {
      case Some(_) => {}
      case None => throw new Exception("Invalid Json file")
    }
  }

  def getVal( keys: String* ): Any = {
    var parsedJson: Any = JSON.parseFull(wholeFile).get
    try {
      for( key <- keys )
        parsedJson = parsedJson.asInstanceOf[Map[String,Any]] (key)
    }
    catch {
      case e: java.lang.ClassCastException =>  throw e
      case e: Throwable => throw(e)
    }
    parsedJson
  }
}
