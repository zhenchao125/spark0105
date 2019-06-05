import java.text.DecimalFormat

/**
  * Author lzc
  * Date 2019-06-05 09:32
  */
object Test {
    def main(args: Array[String]): Unit = {
        val formatter = new DecimalFormat("00000.00")
        println(formatter.format(1.2345))
        println(formatter.format(234.367))
        println(formatter.format(1234.23))
        println(formatter.format(12345.2))
    }
}
