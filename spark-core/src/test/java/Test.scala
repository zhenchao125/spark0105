object Test {
    def main(args: Array[String]): Unit = {
        val arr1 = Array(30, 50, 70, 60, 10, 20)
        
        val arr: Array[Int] = arr1.slice(0, 1)
        arr.foreach(println)
        
    }
}
