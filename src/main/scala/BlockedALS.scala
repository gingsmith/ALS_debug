package als_debug

import java.util.Arrays
import scala.io.Source
import breeze.linalg._
import spark._
import spark.storage.StorageLevel
import java.io._
import scala.util._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.BitSet
import org.jblas.{DoubleMatrix, SimpleBlas, Solve}
// need _ to include everything in package
// reduceByKey in implicit RDD cast
import spark.SparkContext._

// MOST RECENT VERSION: 06/10


/**
 * A blocked version of alternating least squares matrix factorization that groups the two sets
 * of factors (referred to as "users" and "movies") into blocks and reduces communication by only
 * sending one copy of each user vector to each movie block on each iteration, and only for the
 * movie blocks that need that user's feature vector. This is achieved by precomputing some
 * information about the ratings matrix to determine the "out-links" of each user (which blocks of
 * movies it will contribute to) and "in-link" information for each movie (which of the feature
 * vectors it receives from each user block it will depend on). This allows us to send only an
 * array of feature vectors between each user block and movie block, and have the movie block find
 * the users' ratings and update the movies based on these messages.
 */
object BlockedALS {
  /**
   * Out-link information for a user or movie block. This includes the original user/movie IDs
   * of the elements within this block, and the list of destination blocks that each user or
   * movie will need to send its feature vector to.
   */
  private case class OutLinkBlock(elementIds: Array[Int], shouldSend: Array[BitSet])

  /**
   * Make the out-links table for a block of the users (or movies) dataset given the list of
   * (user, movie, rating) values for the users in that block (or the opposite for movies).
   */
  private def makeOutLinkBlock(numBlocks: Int, ratings: Array[(Int, Int, Double)]): OutLinkBlock = {
    val userIds = ratings.map(_._1).distinct.sorted
    val numUsers = userIds.length
    val userIdToPos = userIds.zipWithIndex.toMap
    val shouldSend = Array.fill(numUsers)(new BitSet(numBlocks))
    for ((u, m, r) <- ratings) {
      shouldSend(userIdToPos(u))(m % numBlocks) = true
    }
    OutLinkBlock(userIds, shouldSend)
  }

  /**
   * In-link information for a user (or movie) block. This includes the original user/movie IDs
   * of the elements within this block, as well as an array of indices and ratings that specify
   * which user in the block will be rated by which movies from each movie block (or vice-versa).
   * Specifically, if this InLinkBlock is for users, ratingsForBlock(b)(i) will contain two arrays,
   * indices and ratings, for the i'th movie that will be sent to us by movie block b (call this M).
   * These arrays represent the users that movie M had ratings for (by their index in this block),
   * as well as the corresponding rating for each one. We can thus use this information when we get
   * movie block b's message to update the corresponding users.
   */
  private case class InLinkBlock(
    elementIds: Array[Int], ratingsForBlock: Array[Array[(Array[Int], Array[Double])]])

  /**
   * Make the in-links table for a block of the users (or movies) dataset given a list of
   * (user, movie, rating) values for the users in that block (or the opposite for movies).
   */
  private def makeInLinkBlock(numBlocks: Int, ratings: Array[(Int, Int, Double)]): InLinkBlock = {
    val userIds = ratings.map(_._1).distinct.sorted
    val numUsers = userIds.length
    val userIdToPos = userIds.zipWithIndex.toMap
    val ratingsForBlock = new Array[Array[(Array[Int], Array[Double])]](numBlocks)
    for (movieBlock <- 0 until numBlocks) {
      val ratingsInBlock = ratings.filter(t => t._2 % numBlocks == movieBlock)
      val ratingsByMovie = ratingsInBlock.groupBy(_._2)  // (m, Seq[(u, m, r)])
                             .toArray
                             .sortBy(_._1)
                             .map{case (m, rs) => (rs.map(t => userIdToPos(t._1)), rs.map(_._3))}
      ratingsForBlock(movieBlock) = ratingsByMovie
    }
    InLinkBlock(userIds, ratingsForBlock)
  }

  /**
   * Make RDDs of InLinkBlocks and OutLinkBlocks given an RDD of (blockId, (u, m, r)) values for
   * the users (or (blockId, (m, u, r)) for the movies). We create these simultaneously to avoid
   * having to shuffle the (blockId, (u, m, r)) RDD twice, or to cache it.
   */
  private def makeLinkRDDs(numBlocks: Int, ratings: RDD[(Int, (Int, Int, Double))])
    : (RDD[(Int, InLinkBlock)], RDD[(Int, OutLinkBlock)]) =
  {
    val grouped = ratings.partitionBy(new HashPartitioner(numBlocks))
    val links = grouped.mapPartitionsWithIndex((blockId, elements) => {
      val ratings = elements.map(_._2).toArray
      Iterator((blockId, (makeInLinkBlock(numBlocks, ratings), makeOutLinkBlock(numBlocks, ratings))))
    }, true)
    links.persist()
    (links.mapValues(_._1), links.mapValues(_._2))
  }

  /** Make a random initial factor vector with the given seed. */
  private def makeInitialFactor(rank: Int, seed: Int): Array[Double] = {
    val rand = new Random(seed)
    Array.fill(rank)(rand.nextDouble)
  }
  
  /**
   * Train the ALS model for a given iteration with the given number of features (rank). Returns
   * RDDs of feature vectors for each user and movie.
   */
  def train(numBlocks: Int, ratings: RDD[(Int,Int,Double)], rank: Int, lambda: Double, iters: Int)
    : (RDD[(Int,Array[Double])], RDD[(Int,Array[Double])]) =
  {
    val partitioner = new HashPartitioner(numBlocks)

    val ratingsByUserBlock = ratings.map{ case (u, m, r) => (u % numBlocks, (u, m, r)) }
    val ratingsByMovieBlock = ratings.map{ case (u, m, r) => (m % numBlocks, (m, u, r)) }

    val (userInLinks, userOutLinks) = makeLinkRDDs(numBlocks, ratingsByUserBlock)
    val (movieInLinks, movieOutLinks) = makeLinkRDDs(numBlocks, ratingsByMovieBlock)

    // Initialize user and movie factors deterministically.
    var users = userOutLinks.mapValues(_.elementIds.map(u => makeInitialFactor(rank, u)))
    var movies = movieOutLinks.mapValues(_.elementIds.map(m => makeInitialFactor(rank, -m)))

    for (iter <- 0 until iters) {
      // perform ALS update
      movies = updateFeatures(users, userOutLinks, movieInLinks, partitioner, rank, lambda)
      users = updateFeatures(movies, movieOutLinks, userInLinks, partitioner, rank, lambda)
    }

    // Flatten and cache the two final RDDs to un-block them
    val usersOut = users.join(userOutLinks).flatMap { case (bid, (factors, outLinkBlock)) =>
      for (i <- 0 until factors.length) yield (outLinkBlock.elementIds(i), factors(i))
    }.cache()
    val moviesOut = movies.join(movieOutLinks).flatMap { case (bid, (factors, outLinkBlock)) =>
      for (i <- 0 until factors.length) yield (outLinkBlock.elementIds(i), factors(i))
    }.cache()

    (usersOut, moviesOut)
  }

  /**
   * Compute the user feature vectors given the current movies (or vice-versa). This first joins
   * the movies with their out-links to generate a set of messages to each destination block
   * (specifically, the features for the movies that user block cares about), then groups these
   * by destination and joins them with the in-link info to figure out how to update each user.
   * It returns an RDD of new feature vectors for each user block.
   */
  private def updateFeatures(
      movies: RDD[(Int, Array[Array[Double]])],
      movieOutLinks: RDD[(Int, OutLinkBlock)],
      userInLinks: RDD[(Int, InLinkBlock)],
      partitioner: Partitioner,
      rank: Int,
      lambda: Double)
    : RDD[(Int, Array[Array[Double]])] =
  {
    val numBlocks = movies.partitions.size 
    movieOutLinks.join(movies).flatMap { case (bid, (outLinkBlock, factors)) =>
        val toSend = Array.fill(numBlocks)(new ArrayBuffer[Array[Double]])
        for (m <- 0 until outLinkBlock.elementIds.length; userBlock <- 0 until numBlocks) {
          if (outLinkBlock.shouldSend(m)(userBlock)) {
            toSend(userBlock) += factors(m)
          }
        }
        toSend.zipWithIndex.map{ case (buf, idx) => (idx, (bid, buf.toArray)) }
    }.groupByKey(partitioner)
     .join(userInLinks)
     .mapValues{ case (messages, inLinkBlock) => updateBlock(messages, inLinkBlock, rank, lambda) }
  }

  /**
   * Compute the new feature vectors for a block of the users matrix given the list of factors
   * it received from each movie and its InLinkBlock.
   */
  def updateBlock(messages: Seq[(Int, Array[Array[Double]])], inLinkBlock: InLinkBlock,
      rank: Int, lambda: Double)
    : Array[Array[Double]] =
  {
    // Sort the incoming block factor messages by block ID and make them an array
    val blockFactors = messages.sortBy(_._1).map(_._2).toArray // Array[Array[Double]]
    val numBlocks = blockFactors.length
    val numUsers = inLinkBlock.elementIds.length
    
    // We'll sum up the XtXes using vectors that represent only the lower-triangular part, since
    // the matrices are symmetric
    val triangleSize = rank * (rank + 1) / 2
    val userXtX = Array.fill(numUsers)(DoubleMatrix.zeros(triangleSize))
    val userXy = Array.fill(numUsers)(DoubleMatrix.zeros(rank))

    // Some temp variables to avoid memory allocation
    val tempXtX = DoubleMatrix.zeros(triangleSize)
    val fullXtX = DoubleMatrix.zeros(rank, rank)

    // Compute the XtX and Xy values for each user by adding movies it rated in each movie block
    for (movieBlock <- 0 until numBlocks) {
      for (m <- 0 until blockFactors(movieBlock).length) {
        val x = new DoubleMatrix(blockFactors(movieBlock)(m))
        fillXtX(x, tempXtX)
        val (us, rs) = inLinkBlock.ratingsForBlock(movieBlock)(m)
        for (i <- 0 until us.length) {
          userXtX(us(i)).addi(tempXtX)
          SimpleBlas.axpy(rs(i), x, userXy(us(i)))
        }
      }
    }

    // Solve the least-squares problem for each user and return the new feature vectors
    userXtX.zipWithIndex.map{ case (triangularXtX, index) =>
      // Compute the full XtX matrix from the lower-triangular part we got above
      computeFullMatrix(triangularXtX, fullXtX)
      // Add regularization
      (0 until rank).foreach(i => fullXtX.data(i*rank + i) += lambda)
      // Solve the resulting matrix, which is symmetric and positive-definite
      Solve.solvePositive(fullXtX, userXy(index)).data
    }
  }

  /**
   * Set xtxDest to the lower-triangular part of x transpose * x. For efficiency in summing
   * these matrices, we store xtxDest as only rank * (rank+1) / 2 values, namely the values
   * at (0,0), (1,0), (1,1), (2,0), (2,1), (2,2), etc in that order.
   */
  private def fillXtX(x: DoubleMatrix, xtxDest: DoubleMatrix) {
    var i = 0
    var pos = 0
    while (i < x.length) {
      var j = 0
      while (j <= i) {
        xtxDest.data(pos) = x.data(i) * x.data(j)
        pos += 1
        j += 1
      }
      i += 1
    }
  }

  /**
   * Given a triangular matrix in the order of fillXtX above, compute the full symmetric square
   * matrix that it represents, storing it into destMatrix.
   */
  private def computeFullMatrix(triangularMatrix: DoubleMatrix, destMatrix: DoubleMatrix) {
    val rank = destMatrix.rows
    var i = 0
    var pos = 0
    while (i < rank) {
      var j = 0
      while (j <= i) {
        destMatrix.data(i*rank + j) = triangularMatrix.data(pos)
        destMatrix.data(j*rank + i) = triangularMatrix.data(pos)
        pos += 1
        j += 1
      }
      i += 1
    }
  }


  // def trainALSBlocked(nsplits: Int, ratings: RDD[(Int,Int,Double)], rank: Int, lambda: Double, niter: Int):
  //   (RDD[(Int,Array[Double])], RDD[(Int,Array[Double])]) = {

  //   println("Running blocked als")
  //   matei_BlockedALS.train(nsplits, ratings, rank, lambda, niter)
  // }


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
        .persist(StorageLevel.MEMORY_ONLY_SER)
    }
    else {
      trainData = sc.textFile(trainfile,nsplits)
        .map(_.split(' '))
        .map{elements => (elements(0).toInt-1,elements(1).toInt-1,elements(2).toDouble)}
        .persist(StorageLevel.MEMORY_ONLY_SER)
    }

    println("Number of splits in trainData: " + trainData.partitions.size)

    // Do the actual training
    val (users,movies) = train(trainData.partitions.size, trainData, rank, lambda, niter)

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
