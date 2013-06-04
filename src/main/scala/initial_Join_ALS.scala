package als_debug

import scala.io.Source
import breeze.linalg._
import spark._
import java.io._
import scala.util._
// need _ to include everything in package
// reduceByKey in implicit RDD cast
import spark.SparkContext._


object initial_Join_ALS {


  def trainALSFast(ratings: RDD[(Int,Int,Double)], rank: Int, lambda: Double, niter: Int):
    (RDD[(Int,Array[Double])], RDD[(Int,Array[Double])]) = {

    println("Running fast als")

    val partitioner = new HashPartitioner(ratings.partitions.size)

    val ratingsByUser = ratings.map{ case (u,m,r) => (u,(m,r)) }.partitionBy(partitioner).cache;
    val ratingsByMovie = ratings.map{ case (u,m,r) => (m,(u,r)) }.partitionBy(partitioner).cache;

    def makeInitialFactor(seed: Int): Array[Double] = {
      val rand = new Random(seed)
      Array.fill(rank)(rand.nextDouble)
    }

    // Initialize user and movie factors deterministically.  Map partitions is used to
    // preserve the partitioning achieved when using reduceByKey.
    var users = ratingsByUser.mapValues(x => 1).reduceByKey(_ + _)
      .mapPartitions( _.map{ case (uid, deg) => (uid, makeInitialFactor(uid)) } ).cache
    // Note I use the negative of the movie id to initialize movie factors
    var movies = ratingsByMovie.mapValues(x => 1).reduceByKey(_ + _)
      .mapPartitions( _.map{ case (mid, deg) => (mid, makeInitialFactor(-mid)) } ).cache

    val nnzs = ratingsByUser.count
    val nusers = users.count
    val nmovies = movies.count
    println("Ratings: " + nnzs)
    println("Users:   " + nusers)
    println("Movies:  " + nmovies)

    def computeXtXandXy(ratingX: (Double, Array[Double])) :
      (Array[Double], Array[Double]) = {
      val (rating, x) = ratingX
      val xty = x.map(_ * rating)
      // Computing xtx in upper triangular form
      val xtx = ( for(i <- 0 until rank; j <- i until rank) yield(x(i) * x(j)) ).toArray
      (xtx, xty)
    }

    def foldXtXandXy(sum: (Array[Double], Array[Double]), ratingX: (Double, Array[Double]) ) :
      (Array[Double], Array[Double]) = {
      val (xtxSum, xtySum) = sum
      val (rating, x) = ratingX
      var i = 0
      while(i < rank) {
        xtySum(i) += x(i) * rating
        i += 1
      }
      i = 0
      var index = 0
      while(i < rank) {
        var j = i
        while(j < rank) {
          xtxSum(index) += x(i) * x(j)
          index += 1
          j += 1
        }
        i += 1
      }
      (xtxSum, xtySum)
    }

    def sumXtXandXy(a: (Array[Double], Array[Double]), b: (Array[Double], Array[Double]) ) :
      (Array[Double], Array[Double]) = {
      var i = 0
      while(i < a._1.length) { a._1(i) += b._1(i); i += 1 }
      i = 0
      while(i < a._2.length) { a._2(i) += b._2(i); i += 1 }
      a
    }

    def solveLeastSquares( xtxAr: Array[Double], xtyAr: Array[Double] ) :
      Array[Double] = {
      val xtx = DenseMatrix.tabulate(rank,rank){ (i,j) =>
        xtxAr(if(i <= j) j + i*(rank-1)-(i*(i-1))/2 else i + j*(rank-1)-(j*(j-1))/2) +
        (if(i == j) lambda else 0.0) //regularization
      }
      val xty = DenseMatrix.create(rank, 1, xtyAr)
      val w = xtx \ xty
      w.data
    }

    for(iter <- 0 until niter) {
      // perform ALS update

      movies = ratingsByUser
      .join(users).map{ case (u, ((m, y), x)) => (m, (y,x)) }
      .combineByKey[(Array[Double], Array[Double])](
        computeXtXandXy(_), foldXtXandXy(_, _),  sumXtXandXy(_, _), partitioner)
      .mapValues{ case (xtx, xty) => solveLeastSquares(xtx, xty) }

      // Cache the last movies
      if(iter + 1 == niter) movies = movies.cache()


      users = ratingsByMovie
      .join(movies).map{ case (m, ((u, y), x)) => (u, (y,x)) }
      .combineByKey[(Array[Double], Array[Double])](
        computeXtXandXy(_), foldXtXandXy(_, _),  sumXtXandXy(_, _), partitioner)
      .mapValues{ case (xtx, xty) => solveLeastSquares(xtx, xty) }

      // Cache the last users
      if(iter + 1 == niter) users = users.cache()
    }

    (users, movies)
  }


  def computeError(users: RDD[(Int, Array[Double])], movies: RDD[(Int, Array[Double])],
    ratings: RDD[(Int, Int, Double)]): Double = {

    def error(ux: Array[Double], mx: Array[Double], rating: Double): Double = {
      val pred = ux.view.zip(mx).map{ case (a,b) => (a*b) }.sum
      val error = (pred - rating) * (pred - rating)
      error
    }
    ratings.map{ case (u,m,r) => (u,(m,r)) }.join(users)
    .map{ case (u, ((m,r), ux)) => (m, (ux, r)) }.join(movies)
    .map{ case (m, ((ux, r), mx)) => error(ux, mx, r) }.sum
  }




  def main(args: Array[String]){


    // Add kryo serialization
    //System.setProperty("spark.broadcast.factory", "spark.broadcast.TreeBroadcastFactory")
    //System.setProperty("spark.local.dir", "/mnt")
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "als_debug.CCDKryoRegistrator")
    System.setProperty("spark.storage.blockManagerHeartBeatMs", "120000")



    val options =  args.map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case Array(opt) => (opt -> "true")
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }.toMap

    // read in input
    val master = options.getOrElse("master", "local[4]")
    val trainfile = options.getOrElse("train", "")
    val testfile = options.getOrElse("test", "")
    val rank = options.getOrElse("rank", "10").toInt
    val lambda = options.getOrElse("lambda", "0.01").toDouble
    val niter = options.getOrElse("niter", "10").toInt
    val jar = options.getOrElse("jars", "")
    val nsplits = options.getOrElse("nsplits", "4").toInt
    val sparkhome = options.getOrElse("sparkhome", System.getenv("SPARK_HOME"))
    val big = options.getOrElse("big", "false").toBoolean
    val m = options.getOrElse("m", "100").toInt
    val n = options.getOrElse("n", "100").toInt

    // print out input
    println("master:       " + master)
    println("train:        " + trainfile)
    println("test:         " + testfile)
    println("rank:         " + rank)
    println("lambda:       " + lambda)
    println("niter:        " + niter)
    println("jar:          " + jar)
    println("sparkhome:    " + sparkhome)
    println("nsplits:      " + nsplits)  
    println("big:          " + big)  
    println("m:            " + m)
    println("n:            " + n)

    // Set up spark context
    val sc = new SparkContext(master, "Join_ALS", sparkhome, List(jar))

    var trainData: spark.RDD[(Int,Int,Double)] = null

    if(big){
      trainData = sc.textFile(trainfile,nsplits)
        .map(_.split(' '))
        .map{ elements => (elements(0).toInt-1,elements(1).toInt-1,elements(2).toDouble)}
        .flatMap( x => Array(x,(x._1+m,x._2,x._3),(x._1,x._2+n,x._3),(x._1+m,x._2+n,x._3)))
        .cache
    }
    else {
      trainData = sc.textFile(trainfile, nsplits)
        .map(_.split(' '))
        .map{elements => (elements(0).toInt-1,elements(1).toInt-1,elements(2).toDouble)}
        .cache()
    }

    // Do the actual training
    val (users, movies) = trainALSFast(trainData, rank, lambda, niter)

    // Force computation for timing purposes:
    val starttime = System.currentTimeMillis
    val regularizationCost = lambda *
      (users.map{ case (uid, x) => x.view.map(xi => xi * xi).sum}.sum +
      movies.map{ case (mid, x) => x.view.map(xi => xi * xi).sum}.sum)
    val runtime = System.currentTimeMillis - starttime

    // Compute training error
    val trainingError = computeError(users, movies, trainData)

    // Output Results
    println("Regularization penalty: " + regularizationCost)
    println("Runtime: " + runtime)
    println("Training Error: " + trainingError)
    println("Training Loss:  " + (trainingError + regularizationCost))

    // Compute testing error if desired
    if(!testfile.isEmpty) {
      val testData: spark.RDD[(Int,Int,Double)] = sc.textFile(testfile, nsplits)
        .map(_.split(' ')).map{
          elements => (elements(0).toInt-1,elements(1).toInt-1,elements(2).toDouble)}.cache()
      val testError = computeError(users, movies, testData)
      println("Test Error: " + testError)
    }


    sc.stop()
  }

}
