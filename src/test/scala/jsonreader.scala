import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import java.io._

class JsonReaderTest extends FunSuite
{
  test("Parse simple config file") {
    val filename = "unittestconfig.json"
    try {
      // produce config file
      val writer = new PrintWriter(new File(filename))
      writer.write("{\n")
      writer.write("\t\"a\": \"a\",\n")
      writer.write("\t\"b\": {\n")
      writer.write("\t\t\"c\": \"c\",\n")
      writer.write("\t\t\"d\": {\n")
      writer.write("\t\t\t\"e\": \"e\"\n")
      writer.write("\t\t}\n")
      writer.write("\t},\n")
      writer.write("\t\"a with space\": \"a with space\"\n")
      writer.write("}\n")
      writer.close

      // verify parsing
      val jsonReader = new JsonReader(filename)
      assert( jsonReader.getVal("a").toString === "a" )
      assert( jsonReader.getVal("a with space").toString === "a with space" )
      assert( jsonReader.getVal("b","c").toString === "c" )
      assert( jsonReader.getVal("b","d","e").toString === "e" )

      {
        val thrown = intercept[Exception] {
          jsonReader.getVal("f")
        }
        assert( thrown.getMessage === "key not found: f" )
      }

      {
        val thrown = intercept[Exception] {
          jsonReader.getVal("b","f")
        }
        assert( thrown.getMessage === "key not found: f" )
      }
    }
    finally {
      val file = new File(filename)
      file.delete
    }
  }
}
