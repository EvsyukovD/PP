import java.time.Duration
import scala.annotation.tailrec
import scala.runtime.Nothing$
import scala.specialized
import math.Numeric.Implicits.infixNumericOps
import math.Fractional.Implicits.infixFractionalOps
import math.Integral.Implicits.infixIntegralOps
import scala.math.{ScalaNumber, floor, log, sqrt}
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

trait IMatrix {
  def +(b: Matrix): Matrix = ???

  def -(b: Matrix): Matrix = ???

  def *(b: Matrix): Matrix = ???

  def *(b: Double): Matrix = ???
}

class Matrix extends IMatrix {
  private var m: Int = 0
  private var n: Int = 0
  private var matrix: Array[Array[Double]] = null

  override def toString(): String = {
    val res: StringBuilder = new StringBuilder("")
    matrix.foreach((a: Array[Double]) => {
      a.foreach((x: Double) => {
        res.append(x.toString + " ")
      })
      res.append("\n")
    })
    res.toString()
  }

  def getRowsNum(): Int = {
    m
  }

  def getColumnsNum(): Int = {
    n
  }

  override def +(b: Matrix): Matrix = {
    assert(b.m == m && b.n == n, "Matrixes must have equal sizes")
    val res = Matrix(m, n)
    (0 until m).foreach(i => {
      (0 until n).foreach(j => {
        val x = matrix(i)(j)
        res.matrix(i)(j) = x.+(b.matrix(i)(j))
      })
    })
    res
  }

  override def -(b: Matrix): Matrix = {
    assert(b.m == m && b.n == n, "Matrixes must have equal sizes")
    val res = Matrix(m, n)
    (0 until m).foreach(i => {
      (0 until n).foreach(j => {
        res.matrix(i)(j) = matrix(i)(j) - b.matrix(i)(j)
      })
    })
    res
  }

  override def *(b: Matrix): Matrix = {
    assert(n == b.m, "The number of columns in first matrix must be equal number of rows in second matrix")
    val res = Matrix(m, n)
    (0 until m).foreach(i => {
      (0 until n).foreach(j => {
        (0 until n).foreach(k => {
          res.matrix(i)(j) += matrix(i)(k) * b.matrix(k)(j)
        })
      })
    })
    res
  }

  override def *(b: Double): Matrix = {
    val res = Matrix(m, n)
    (0 until m).foreach(i => {
      (0 until n).foreach(j => {
        res.matrix(i)(j) = matrix(i)(j) * b
      })
    })
    res
  }

  def at(i: Int): Array[Double] = matrix(i)
}

object Matrix {
  def apply(rows: Int, columns: Int): Matrix = {
    val res: Matrix = new Matrix
    res.m = rows
    res.n = columns
    res.matrix = new Array[Array[Double]](res.m)

    def createRows(m: Matrix): Matrix = {
      @tailrec
      def create(i: Int): Matrix = {
        if (i < 0) {
          res
        }
        else {
          res.matrix(i) = new Array(res.n)
          create(i - 1)
        }
      }

      create(res.m - 1)
    }

    createRows(res)
  }

  def apply(): Matrix = {
    new Matrix
  }

  def apply(m: Array[Array[Double]]): Matrix = {
    val _m = m.length
    val _n = m(0).length
    //is not sparse matrix
    (0 until _m).foreach(i => {
      assert(_n == m(i).length, "Matrix can't be sparse")
    })
    //if not sparse matrix
    val res = Matrix(_m, _n)
    (0 until _m).foreach(i => {
      (0 until _n).foreach(j => {
        res.at(i)(j) = m(i)(j)
      })
    })
    res
  }
  /**
   * Parallel sum of matrixes
   * @param a  first matrix
   * @param b  second matrix
   * @param processNum  number of processes that sum rows
   * @return sum of matrixes
   * */
  def parallelSum(a: Matrix, b: Matrix,processNum: Int = 1): Matrix = {
    assert(a.m == b.m && a.n == b.n, "Matrixes must have equal sizes")
    assert(processNum > 0,"Process number must be positive")
    val res = Matrix(a.m, a.n)

    def getTasks(startIndex: Int, endIndex: Int, matr1: Matrix, matr2: Matrix, res: Matrix): Seq[Future[Unit]] = {
      for (i <- startIndex until endIndex) yield Future {
        for (j <- 0 until matr1.n) {
          res.matrix(i)(j) = matr1.matrix(i)(j) + matr2.matrix(i)(j)
        }
      }
    }

    val r: Int = a.m % processNum
    val N: Int = (a.m - r) / processNum
    for (i <- 0 until N) {
      //getTasks(i * processNum, (i + 1) * processNum, a, b, res)
      val aggregated: Future[Seq[Unit]] = Future.sequence(getTasks(i * processNum, (i + 1) * processNum, a, b, res))
      Await.result(aggregated, scala.concurrent.duration.Duration.Inf)
    }
    //getTasks(N * processNum, r + N * processNum, a, b, res)
    val aggregated: Future[Seq[Unit]] = Future.sequence(getTasks(N * processNum, r + N * processNum, a, b, res))
    Await.result(aggregated, scala.concurrent.duration.Duration.Inf)
    res
  }
  /**
   * Parallel subtract of matrixes
   * @param a  first matrix
   * @param b  second matrix
   * @param processNum  number of processes that subtract rows
   * @return subtract of matrixes
   * */
  def parallelSubtract(a: Matrix, b: Matrix,processNum: Int = 1): Matrix = {
    assert(a.m == b.m && a.n == b.n, "Matrixes must have equal sizes")
    assert(processNum > 0,"Process number must be positive")
    val res = Matrix(a.m, a.n)

    def getTasks(startIndex: Int, endIndex: Int, matr1: Matrix, matr2: Matrix, res: Matrix): Seq[Future[Unit]] = {
      for (i <- startIndex until endIndex) yield Future {
        for (j <- 0 until matr1.n) {
          res.matrix(i)(j) = matr1.matrix(i)(j) - matr2.matrix(i)(j)
        }
      }
    }

    val r: Int = a.m % processNum
    val N: Int = (a.m - r) / processNum
    for (i <- 0 until N) {
      //doTasks(i * processNum, (i + 1) * processNum, a, b, res)
      val aggregated: Future[Seq[Unit]] = Future.sequence(getTasks(i * processNum, (i + 1) * processNum, a, b, res))
      Await.result(aggregated, scala.concurrent.duration.Duration.Inf)
    }
    //doTasks(N * processNum, r + N * processNum, a, b, res)
    val aggregated: Future[Seq[Unit]] = Future.sequence(getTasks(N * processNum, r + N * processNum, a, b, res))
    Await.result(aggregated, scala.concurrent.duration.Duration.Inf)
    res
  }
  /**
   * Parallel mul of squad matrixes
   * by dividing them on blocks
   * @param a  first matrix
   * @param b  second matrix
   * @param blockSize  the size of each block
   * @return mul of matrixes
   * */
  def parallelMulOfSquadMatrixes(a: Matrix, b: Matrix, blockSize: Int = 1): Matrix = {
    assert(a.m == a.n && b.m == b.n, "Not a squad matrixes")
    assert(a.n == b.m, "The number of columns in first matrix must be equal number of rows in second matrix")
    assert(blockSize > 0, "Block size must be positive")
    val r: Int = a.m % blockSize
    assert(r == 0, "The matrix size is not entirely divisible by block size")
    val res = Matrix(a.m, a.n)
    val q: Int = a.m / blockSize

    def calc_ij_block(i: Int, j: Int, matr1: Matrix, matr2: Matrix, r: Matrix, blockSize: Int): Unit = {
      val _start_i: Int = i * blockSize
      val _end_i: Int = (i + 1) * blockSize
      val _start_j: Int = j * blockSize
      val _end_j: Int = (j + 1) * blockSize
      var Cij: Matrix = Matrix(blockSize, blockSize)
      val slice = (ar: Array[Array[Double]], i: Int, j: Int, blockSize: Int) => {
        val res: Array[Array[Double]] = ar.slice(i * blockSize, (i + 1) * blockSize)
        for (s <- 0 until res.length) {
          res(s) = res(s).slice(j * blockSize, (j + 1) * blockSize)
        }
        res
      }
      val n = matr1.m / blockSize
      for (s <- 0 until n) {
        val Ais: Matrix = Matrix(slice(matr1.matrix, i, s, blockSize))
        val Bsj: Matrix = Matrix(slice(matr2.matrix, s, j, blockSize))
        val tmp: Matrix = Ais * Bsj
        Cij = Cij + tmp
      }
      for (u <- _start_i until _end_i) {
        for (w <- _start_j until _end_j) {
          r.at(u)(w) = Cij.at(u - _start_i)(w - _start_j)
        }
      }
    }

    def getTasks(matr1: Matrix, matr2: Matrix, r: Matrix, blockSize: Int): Seq[Future[Unit]] = {
      val n = matr1.m / blockSize
      for {i <- 0 until n
           j <- 0 until n} yield Future {
        calc_ij_block(i, j, matr1, matr2, r, blockSize)
      }
    }
    val seq = getTasks(a, b, res, blockSize)
    val aggregated: Future[Seq[Unit]] = Future.sequence(seq)
    Await.result(aggregated, scala.concurrent.duration.Duration.apply(10000, "millis"))
    res
  }
  /**
   * Block mul of squad matrixes in one thread
   * @param a  first matrix
   * @param b  second matrix
   * @param blockSize  the size of each block
   * @return mul of matrixes
   * */
  def blockMul(a: Matrix,b: Matrix,blockSize: Int = 1): Matrix = {
    assert(a.m == a.n && b.m == b.n, "Not a squad matrixes")
    assert(a.n == b.m, "The number of columns in first matrix must be equal number of rows in second matrix")
    assert(blockSize > 0, "Block size must be positive")
    val r: Int = a.m % blockSize
    assert(r == 0, "The matrix size is not entirely divisible by block size")
    val res = Matrix(a.m, a.n)
    val q: Int = a.m / blockSize

    def calc_ij_block(i: Int, j: Int, matr1: Matrix, matr2: Matrix, r: Matrix, blockSize: Int): Unit = {
      val _start_i: Int = i * blockSize
      val _end_i: Int = (i + 1) * blockSize
      val _start_j: Int = j * blockSize
      val _end_j: Int = (j + 1) * blockSize
      var Cij: Matrix = Matrix(blockSize, blockSize)
      val slice = (ar: Array[Array[Double]], i: Int, j: Int, blockSize: Int) => {
        val res: Array[Array[Double]] = ar.slice(i * blockSize, (i + 1) * blockSize)
        for (s <- 0 until res.length) {
          res(s) = res(s).slice(j * blockSize, (j + 1) * blockSize)
        }
        res
      }
      val n = matr1.m / blockSize
      for (s <- 0 until n) {
        val Ais: Matrix = Matrix(slice(matr1.matrix, i, s, blockSize))
        val Bsj: Matrix = Matrix(slice(matr2.matrix, s, j, blockSize))
        val tmp: Matrix = Ais * Bsj
        Cij = Cij + tmp
      }
      for (u <- _start_i until _end_i) {
        for (w <- _start_j until _end_j) {
          r.at(u)(w) = Cij.at(u - _start_i)(w - _start_j)
        }
      }
    }

    def calcC(matr1: Matrix, matr2: Matrix, r: Matrix, blockSize: Int): Unit = {
      val n = matr1.m / blockSize
      for {i <- 0 until n
           j <- 0 until n} {
        calc_ij_block(i, j, matr1, matr2, r, blockSize)
      }
    }
    calcC(a, b, res, blockSize)
    res
  }
  /**
   * Deafult mul of 2 squad matrixes with parallel
   * calculating rows
   * @param a  first matrix
   * @param b  second matrix
   * @param processNum  number of processes that calculate rows
   * @return mul of matrixes
   * */
  def defaultParallelMul(a: Matrix,b: Matrix,processNum: Int = 1): Matrix ={
    assert(a.m == a.n && b.m == b.n, "Not a squad matrixes")
    assert(a.n == b.m, "The number of columns in first matrix must be equal number of rows in second matrix")
    assert(processNum > 0,"Process number must be positive")
    val res: Matrix = Matrix(a.n,a.m)
    val r: Int = a.m % processNum
    val n: Int = ((a.m - r) / processNum).toInt
    def createTask(matr1: Matrix, matr2: Matrix, res: Matrix,startIndex: Int,endIndex: Int) = {
        Future{
            for(i <- startIndex until endIndex) {
                for (j <- 0 until matr1.n) {
                     for (k <- 0 until matr1.n) {
                          res.matrix(i)(j) += matr1.at(i)(k) * matr2.at(k)(j)
                     }
                }
            }
        }
    }
    def getTasks(matr1: Matrix, matr2: Matrix, res: Matrix, blockNum: Int,procNum: Int): Seq[Future[Unit]] = {
        for(i <- 0 until blockNum) yield createTask(matr1,matr2,res,i * procNum,(i + 1) * procNum)
    }
    val aggregated = Future.sequence(getTasks(a,b,res,n,processNum))
    Await.result(aggregated,scala.concurrent.duration.Duration.apply(10000, "millis"))
    val rest = Future.sequence(Seq(createTask(a,b,res,n * processNum,n * processNum + r)))
    Await.result(rest,scala.concurrent.duration.Duration.apply(10000, "millis"))
    res
  }

  /**
   * Get unit matrix
   * @param size size of matrix
   * @return unit matrix
   * */
  def getE(size: Int): Matrix = {
    assert(size > 0, "Size must be positive")
    val res: Matrix = Matrix(size, size)
    for (i <- 0 until size) {
      res.at(i)(i) = 1.0
    }
    res
  }
  /**
   * Get array of degrees of given matrix starting with 0
   * @param a given matrix
   * @param n max degree of matrix
   * @param blockSize - block size of matrix for parallel multiple
   * @return array of a degrees
   * */
  def getPowArray(a: Matrix, n: Int, blockSize: Int = 1): Array[Matrix] = {
    assert(a.m == a.n, "Matrix must be squad")
    val res: Array[Matrix] = new Array[Matrix](n + 1)
    var tmp: Matrix = Matrix.getE(a.m)
    res(0) = tmp
    for (i <- 1 until n + 1) {
      tmp = Matrix.parallelMulOfSquadMatrixes(tmp, a, blockSize)
      res(i) = tmp
    }
    res
  }

  /**
   * Init polynom function
   * @param coeff  given coefficients of polynom
   * @param blockSizeForMul  block size for parallel multiple
   * @param processNumForSum  process number for parallel sum
   * @param a  given squad matrix
   * @return value of polynom
   * */
  def calcPolynom(coeff: Array[Double], blockSizeForMul: Int = 1, processNumForSum: Int = 1)(a: Matrix): Matrix = {
    assert(coeff != null, "Invalid coefficient's array")
    assert(a.m == a.n, "Matrix must be squad")
    val n: Int = coeff.length
    val powers: Array[Matrix] = Matrix.getPowArray(a, n - 1, blockSizeForMul)
    var res: Matrix = Matrix(a.m, a.n)
    for (i <- 0 until powers.length) {
         res = Matrix.parallelSum(res,powers(i) * coeff(i),processNumForSum)
    }
    res
  }
}

