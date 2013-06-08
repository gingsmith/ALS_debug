package als_debug

import java.util.Arrays
import scala.io.Source
import breeze.linalg._
import spark._
import spark.storage.StorageLevel
import java.io._
import scala.util._
import scala.collection.mutable.ArrayBuffer
import org.jblas.{DoubleMatrix, Solve}
// need _ to include everything in package
// reduceByKey in implicit RDD cast
import spark.SparkContext._


object BlockedALS {

  case class OutLinkBlock(elementIds: Array[Int], shouldSend: Array[Array[Boolean]]) {
    override def toString = "OLB" + (elementIds.toSeq, shouldSend.toSeq.map(_.toSeq)).toString
  }

  case class InLinkBlock(elementIds: Array[Int], ratingsForBlock: Array[Array[(Array[Int], Array[Double])]]) {}

  // Make the out-links table for each block of the users (or movies) dataset given a list of
  // (userBlockID, (user, movie, rating)) values (or the opposite for movies).
  def makeOutLinks(numBlocks: Int, grouped: RDD[(Int, (Int, Int, Double))]): RDD[(Int, OutLinkBlock)] = {
    grouped.mapPartitionsWithIndex((blockId, elements) => {
      val ratings = elements.map(_._2).toArray  // Since we need to iterate through them several times
      val userIds = ratings.map(_._1).distinct.sorted
      val numUsers = userIds.length
      val userIdToPos = userIds.zipWithIndex.toMap
      val shouldSend = Array.fill(numUsers, numBlocks)(false)
      for ((u, m, r) <- ratings) {
        shouldSend(userIdToPos(u))(m % numBlocks) = true
      }
      Iterator((blockId, OutLinkBlock(userIds, shouldSend)))
    }, true).cache()
  }

  // Make the in-links table for each block of the users (or movies) dataset given a list of
  // (userBlockID, (user, movie, rating)) values (or the opposite for movies).
  def makeInLinks(numBlocks: Int, grouped: RDD[(Int, (Int, Int, Double))]): RDD[(Int, InLinkBlock)] = {
    grouped.mapPartitionsWithIndex((blockId, elements) => {
      val ratings = elements.map(_._2).toArray  // Since we need to iterate through them several times
      val userIds = ratings.map(_._1).distinct.sorted
      val numUsers = userIds.length
      val userIdToPos = userIds.zipWithIndex.toMap
      val ratingsForBlock = new Array[Array[(Array[Int], Array[Double])]](numBlocks)
      for (movieBlock <- 0 until numBlocks) {
        val ratingsInBlock = ratings.filter(e => e._2 % numBlocks == movieBlock)
        val ratingsByMovie = ratingsInBlock.groupBy(_._2)  // (m, (u, m, r)*)
                                           .toArray
                                           .sortBy(_._1)
                                           .map{ case (m, els) => (els.map(e => userIdToPos(e._1)), els.map(_._3)) }
        ratingsForBlock(movieBlock) = ratingsByMovie
      }
      Iterator((blockId, InLinkBlock(userIds, ratingsForBlock)))
    }, true).cache()
  }
  
  def train(numBlocks: Int, ratings: RDD[(Int,Int,Double)], rank: Int, lambda: Double, niter: Int):
    (RDD[(Int,Array[Double])], RDD[(Int,Array[Double])]) = {

    val partitioner = new HashPartitioner(numBlocks)

    val groupedUsers = ratings.map{ case (u,m,r) => (u % numBlocks, (u, m, r)) }.partitionBy(partitioner)
    val groupedMovies = ratings.map{ case (u,m,r) => (m % numBlocks, (m, u, r)) }.partitionBy(partitioner)

    val outLinksByUser = makeOutLinks(numBlocks, groupedUsers)
    val inLinksByUser = makeInLinks(numBlocks, groupedUsers)
    val outLinksByMovie = makeOutLinks(numBlocks, groupedMovies)
    val inLinksByMovie = makeInLinks(numBlocks, groupedMovies)

    //println("# of partitions in outLinks: " + outLinksByUser.partitions.size)
    //println("# of partitions in inLinks: " + inLinksByUser.partitions.size)
    //println("USER_MOVIE_RATINGS: " + ratings.collect().toSeq)
    //println("GROUPED_USERS: " + groupedUsers.collect().toSeq)
    //println("GROUPED_USERS_G: " + groupedUsers.glom.map(_.toSeq).collect().toSeq)
    //println("USER_OUT: " + outLinksByUser.collect().toSeq)
    //println("USER_IN: " + inLinksByUser.collect().toSeq)
    //println("GROUPED_MOVIES: " + groupedUsers.collect().toSeq)
    //println("GROUPED_MOVIES_G: " + groupedMovies.glom.map(_.toSeq).collect().toSeq)
    //println("MOVIE_OUT: " + outLinksByMovie.collect().toSeq)
    //println("MOVIE_IN: " + inLinksByMovie.collect().toSeq)

    def makeInitialFactor(seed: Int): Array[Double] = {
      val rand = new Random(seed)
      Array.fill(rank)(rand.nextDouble)
    }

    // Initialize user and movie factors deterministically.
    var users = outLinksByUser.mapValues(outLinkBlock => outLinkBlock.elementIds.map(u => makeInitialFactor(u)))
    var movies = outLinksByMovie.mapValues(outLinkBlock => outLinkBlock.elementIds.map(m => makeInitialFactor(-m)))

    //println("USERS: " + users.collect().toSeq)
    //println("MOVIES: " + movies.collect().toSeq)

    for(iter <- 0 until niter) {
      // perform ALS update
      movies = updateFeatures(users, outLinksByUser, inLinksByMovie, partitioner, rank, lambda)
      users = updateFeatures(movies, outLinksByMovie, inLinksByUser, partitioner, rank, lambda)
    }

    // Flatten and cache the two final RDDs to un-block them
    val usersOut = users.join(outLinksByUser).flatMap { case (bid, (factors, outLinkBlock)) =>
      for (i <- 0 until factors.length) yield (outLinkBlock.elementIds(i), factors(i))
    }.cache()
    val moviesOut = movies.join(outLinksByMovie).flatMap { case (bid, (factors, outLinkBlock)) =>
      for (i <- 0 until factors.length) yield (outLinkBlock.elementIds(i), factors(i))
    }.cache()

    (usersOut, moviesOut)
  }

  def computeXtXandXy(ratingX: (Double, Array[Double])) :
    (Array[Double], Array[Double]) = {
    val (rating, x) = ratingX
    val xty = x.map(_ * rating)
    val rank = x.length
    // Computing xtx in upper triangular form
    val xtx = ( for(i <- 0 until rank; j <- i until rank) yield(x(i) * x(j)) ).toArray
    (xtx, xty)
  }

  def foldXtXandXy(sum: (Array[Double], Array[Double]), ratingX: (Double, Array[Double]) ) :
    (Array[Double], Array[Double]) = {
    val (xtxSum, xtySum) = sum
    val (rating, x) = ratingX
    val rank = x.length
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

  def solveLeastSquares(xtxAr: Array[Double], xtyAr: Array[Double], lambda: Double) :
    Array[Double] = {
    val rank = xtyAr.length
    val xtx = DenseMatrix.tabulate(rank,rank){ (i,j) =>
      xtxAr(if(i <= j) j + i*(rank-1)-(i*(i-1))/2 else i + j*(rank-1)-(j*(j-1))/2) +
      (if(i == j) lambda else 0.0) //regularization
    }
    val xty = DenseMatrix.create(rank, 1, xtyAr)
    val w = xtx \ xty
    w.data
  }

  def fillXtX(xtxDest: DoubleMatrix, x: Array[Double]) {
    var i = 0
    var pos = 0
    while (i < x.length) {
      var j = 0
      while (j <= i) {
        xtxDest.data(pos) = x(i) * x(j)
        pos += 1
        j += 1
      }
      i += 1
    }
  }

  // Compute the user feature vectors given the current movies (or vice-versa).
  def updateFeatures(movies: RDD[(Int, Array[Array[Double]])],
      outLinksByMovie: RDD[(Int, OutLinkBlock)],
      inLinksByUser: RDD[(Int, InLinkBlock)],
      partitioner: Partitioner,
      rank: Int,
      lambda: Double
      ): RDD[(Int, Array[Array[Double]])] =
  {
    val numBlocks = movies.partitions.size 
    outLinksByMovie.join(movies).flatMap { case (bid, (outLinkBlock, factors)) =>
        val toSend = Array.fill(numBlocks)(new ArrayBuffer[Array[Double]])
        for (userBlock <- 0 until numBlocks; m <- 0 until outLinkBlock.elementIds.length) {
          if (outLinkBlock.shouldSend(m)(userBlock)) {
            toSend(userBlock) += factors(m)
          }
        }
        toSend.zipWithIndex.map{ case (buf, idx) => (idx, (bid, buf.toArray)) }
    }.groupByKey(partitioner)
     .join(inLinksByUser)
     .mapValues{ case (blockFactors, inLinkBlock) =>
        val sortedBlockFactors = blockFactors.sortBy(_._1).map(_._2).toArray // Array[Array[Double]]
        val numUsers = inLinkBlock.elementIds.length
        
        val xtxSize = rank * (rank + 1) / 2
        val userXtX = Array.fill(numUsers)(DoubleMatrix.zeros(xtxSize))
        val userXy = Array.fill(numUsers)(DoubleMatrix.zeros(rank))
        val tempXtX = DoubleMatrix.zeros(xtxSize)
        val fullXtX = DoubleMatrix.zeros(rank, rank)

        for (movieBlock <- 0 until numBlocks) {
          val blockFactors = sortedBlockFactors(movieBlock)
          val blockRatings = inLinkBlock.ratingsForBlock(movieBlock)
          for (m <- 0 until blockFactors.length) {
            val x = blockFactors(m)
            fillXtX(tempXtX, x)
            val (us, rs) = blockRatings(m)
            for (i <- 0 until us.length) {
              // Add XtX to userXtX
              var k = 0
              while (k < xtxSize) {
                userXtX(us(i)).data(k) += tempXtX.data(k)
                k += 1
              }
              // Add Xy to userXy
              var j = 0
              while (j < rank) {
                userXy(us(i)).data(j) += x(j) * rs(i)
                j += 1
              }
            }
          }
        }

        userXtX.zipWithIndex.map{ case (uXtX, index) =>
          val uXy = userXy(index)
          // Compute the full XtX matrix from the lower-triangular part uXtX
          var i = 0
          var pos = 0
          while (i < rank) {
            var j = 0
            while (j <= i) {
              fullXtX.data(i*rank + j) = uXtX.data(pos)
              fullXtX.data(j*rank + i) = uXtX.data(pos)
              pos += 1
              j += 1
            }
            i += 1
          }
          // Add regularization
          for (i <- 0 until rank) {
            fullXtX.data(i*rank + i) += lambda
          }
          Solve.solvePositive(fullXtX, uXy).data
        }
    }
  }
}


object Join_ALS {


  def trainALSFast(ratings: RDD[(Int,Int,Double)], rank: Int, lambda: Double, niter: Int):
    (RDD[(Int,Array[Double])], RDD[(Int,Array[Double])]) = {

    println("Running fast als")

    val partitioner = new HashPartitioner(ratings.partitions.size)

    val ratingsByUser = ratings.map{ case (u,m,r) => (u,(m,r)) }
                               .partitionBy(partitioner)
                               .persist(StorageLevel.MEMORY_ONLY_SER)
    val ratingsByMovie = ratings.map{ case (u,m,r) => (m,(u,r)) }
                                .partitionBy(partitioner)
                                .persist(StorageLevel.MEMORY_ONLY_SER)

    def makeInitialFactor(seed: Int): Array[Double] = {
      val rand = new Random(seed)
      Array.fill(rank)(rand.nextDouble)
    }

    // Initialize user and movie factors deterministically.  Map partitions is used to
    // preserve the partitioning achieved when using reduceByKey.
    var users = ratingsByUser.mapValues(x => 1).reduceByKey(_ + _)
      .mapPartitions( _.map{ case (uid, deg) => (uid, makeInitialFactor(uid)) }, true ).cache
    // Note I use the negative of the movie id to initialize movie factors
    var movies = ratingsByMovie.mapValues(x => 1).reduceByKey(_ + _)
      .mapPartitions( _.map{ case (mid, deg) => (mid, makeInitialFactor(-mid)) }, true ).cache

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

    // Given the ratings and features of the users for a movie (or vice-versa),
    // update that movie's (user's) vector through a least-squares calculation
    def updateFeatures(data: Seq[(Double, Array[Double])]): Array[Double] = {
      // For now, just call the methods above
      val (xtx, xy) = computeXtXandXy(data.head)
      data.tail.foreach(elem => foldXtXandXy((xtx, xy), elem))
      solveLeastSquares(xtx, xy)
    }

    for(iter <- 0 until niter) {
      // perform ALS update

      //movies = ratingsByUser
      //.join(users).map{ case (u, ((m, y), x)) => (m, (y,x)) }
      //.combineByKey[(Array[Double], Array[Double])](
      //  computeXtXandXy(_), foldXtXandXy(_, _),  sumXtXandXy(_, _), partitioner)
      //.mapValues{ case (xtx, xty) => solveLeastSquares(xtx, xty) }

      movies = ratingsByUser
      .join(users).map{ case (u, ((m, y), x)) => (m, (y,x)) }
      .groupByKey(partitioner)
      .mapValues{ updateFeatures(_) } 

      // Cache the last movies
      if(iter + 1 == niter) movies = movies.cache()

      //users = ratingsByMovie
      //.join(movies).map{ case (m, ((u, y), x)) => (u, (y,x)) }
      //.combineByKey[(Array[Double], Array[Double])](
      //  computeXtXandXy(_), foldXtXandXy(_, _),  sumXtXandXy(_, _), partitioner)
      //.mapValues{ case (xtx, xty) => solveLeastSquares(xtx, xty) }

      users = ratingsByMovie
      .join(movies).map{ case (m, ((u, y), x)) => (u, (y,x)) }
      .groupByKey(partitioner)
      .mapValues{ updateFeatures(_) } 

      // Cache the last users
      if(iter + 1 == niter) users = users.cache()
    }

    (users, movies)
  }

  def trainALSBlocked(nsplits: Int, ratings: RDD[(Int,Int,Double)], rank: Int, lambda: Double, niter: Int):
    (RDD[(Int,Array[Double])], RDD[(Int,Array[Double])]) = {

    println("Running blocked als")
    BlockedALS.train(nsplits, ratings, rank, lambda, niter)
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

  def replicate(x: (Int,Int,Double), repfact: Int, m:Int, n:Int): Array[(Int,Int,Double)] = {
    val ret_arr = new Array[(Int,Int,Double)](repfact*repfact)
    for(i<-0 until repfact){
      for(j<-0 until repfact){
        val ind = i*repfact+j
        ret_arr(ind) = (x._1+i*m,x._2+j*n,x._3)
      }    
    }
    return ret_arr
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
    val repfact = options.getOrElse("repfact", "1").toInt
    val m = options.getOrElse("m", "100").toInt
    val n = options.getOrElse("n", "100").toInt
    val blocked = options.getOrElse("blocked", "false").toBoolean

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
    println("repfact:          " + repfact)  
    println("m:            " + m)
    println("n:            " + n)
    println("blocked:      " + blocked)  

    // Set up spark context
    val sc = new SparkContext(master, "Join_ALS", sparkhome, List(jar))

    var trainData: spark.RDD[(Int,Int,Double)] = null

    //if(big){
    trainData = sc.textFile(trainfile,nsplits)
      .map(_.split(' '))
      .map{ elements => (elements(0).toInt-1,elements(1).toInt-1,elements(2).toDouble)}
      .flatMap( x => replicate(x,repfact,m,n))
      //Array(x,(x._1+m,x._2,x._3),(x._1,x._2+n,x._3),(x._1+m,x._2+n,x._3)))
      .persist(StorageLevel.MEMORY_ONLY_SER)
    //}
    //else {
    //   trainData = sc.textFile(trainfile, nsplits)
    //     .map(_.split(' '))
    //     .map{elements => (elements(0).toInt-1,elements(1).toInt-1,elements(2).toDouble)}
    //     .persist(StorageLevel.MEMORY_ONLY_SER)
    //}

    // force data to load so we don't count this cost?
    trainData.foreach{x=>()}

    println("Number of splits in trainData: " + trainData.partitions.size)

    // Do the actual training
    val (users, movies) = if (blocked) {
      trainALSBlocked(nsplits, trainData, rank, lambda, niter)
    } else {
      trainALSFast(trainData, rank, lambda, niter)
    }

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
