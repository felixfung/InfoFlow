import java.io.FileReader
import java.io.BufferedReader

class Config( fileName: String ) {
  /***************************************************************************
   * Object to read config file
   ***************************************************************************/

  val( pajekFile, outputDir, mergeAlgo, dampingFactor ) = {

  /***************************************************************************
   * read and parse JSon file content
   ***************************************************************************/
    // read JSon content
    val jsonContent = {
      val reader = new BufferedReader( new FileReader(fileName) )
      var line = reader.readLine
      var wholefile = line
      var looping = true
      while(looping) {
        line = reader.readLine
        looping = ( line != null )
        if(looping)
          wholefile += line
      }
      reader.close
      wholefile
    }

    // parse JSon content
    val json = scala.util.parsing.json.JSON.parseFull(jsonContent)

  /***************************************************************************
   * grab data
   ***************************************************************************/
    val pajekFile: String = json match {
      case Some(m: Map[String, Any]) => m("pajek") match {
        case s: String => s
      }
    }

    val outputDir: String = json match {
      case Some(m: Map[String, Any]) => m("Output") match {
        case s: String => s
      }
    }

    val mergeAlgo: String = json match {
      case Some(m: Map[String, Any]) => m("Algo") match {
        case s: String => s
      }
    }

    val dampingFactor: Double = json match {
      case Some(m: Map[String, Any]) => m("damping") match {
        case s: String => s.toDouble
      }
    }

    ( pajekFile, outputDir, mergeAlgo, dampingFactor )
  }
}
