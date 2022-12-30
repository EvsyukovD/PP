import java.io.BufferedReader
import java.io.FileReader
import scala.util.Random

object MatrixInitializer {
  def apply(fileName: String): Matrix = {
    val fileReader = new BufferedReader(new FileReader(fileName))

    def handleRead(fileReader: BufferedReader): Array[Array[Double]] = {
      var newLine = fileReader.readLine()
      if (newLine == null) {
        return null
      }
      val rowsNum = newLine.toInt
      val res: Array[Array[Double]] = new Array[Array[Double]](rowsNum)
      newLine = fileReader.readLine()
      var k = 0
      while (newLine != null && k < rowsNum) {
        res(k) = newLine.split(" ").map(s => s.toDouble)
        newLine = fileReader.readLine()
        k += 1
      }
      res
    }

    val res = Matrix(handleRead(fileReader))
    fileReader.close()
    res
  }

  def genRandomMatrix(m: Int, n: Int): Matrix = {
    assert(n > 0 && m > 0, "m and n must be positive")
    val res: Matrix = Matrix(m, n)
    val rand = new Random()
    for {i <- 0 until m
         j <- 0 until n}{
         res.at(i)(j) = rand.nextDouble()
    }
    res
  }
}

object MatrixTests extends App {
  def time[R](block: => R): Long = {
    val t0 = System.nanoTime()
    val result = block
    val delta = System.nanoTime() - t0
    delta
  }

  val n: Int = 60
  val m: Int = n
  val NumOfSumIterations: Int = 30
  val processNumForSum: Int = n
  var avg_sum_default: Double = 0.0
  var avg_sum_parallel: Double = 0.0
  for (i <- 0 until NumOfSumIterations) {
       val a: Matrix = MatrixInitializer.genRandomMatrix(m, n)
       val b: Matrix = MatrixInitializer.genRandomMatrix(m, n)
       avg_sum_default = avg_sum_default + time{a + b}
       avg_sum_parallel = avg_sum_parallel + time{Matrix.parallelSum(a,b,processNumForSum)}
  }
  avg_sum_default /= (NumOfSumIterations * 1000000)
  avg_sum_parallel /= (NumOfSumIterations * 1000000)
  println(s"Average time for sum (default, ms): $avg_sum_default")
  println(s"Average time for sum (parallel, ms): $avg_sum_parallel")
  println(s"Num of processes: $processNumForSum")

  var avg_mul_default: Double = 0.0
  var avg_mul_parallel: Double = 0.0
  val NumOfMulIterations: Int = 30
  val blockSize: Int = 50
  val size: Int = 150
  val NumOfProcesses: Int = size * size / (blockSize * blockSize)
  for (i <- 0 until NumOfMulIterations) {
    val a: Matrix = MatrixInitializer.genRandomMatrix(size, size)
    val b: Matrix = MatrixInitializer.genRandomMatrix(size, size)
    avg_mul_default = avg_mul_default + time{a * b}
    avg_mul_parallel = avg_mul_parallel + time{Matrix.parallelMulOfSquadMatrixes(a, b, blockSize)}
  }
  avg_mul_default /= (NumOfMulIterations * 1000000)
  avg_mul_parallel /= (NumOfMulIterations * 1000000)
  println(s"Average time for mul (default, ms): $avg_mul_default")
  println(s"Average time for mul (parallel, ms): $avg_mul_parallel")
  println(s"Num of Processes: $NumOfProcesses")
}
