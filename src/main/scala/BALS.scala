package als_debug

import java.util.Arrays
import scala.io.Source
//import breeze.linalg._
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


object Broadcast_ALS {

   def main(args: Array[String]){

    // sbt/sbt "run-main dmf.Run_ALS_Spark master data/ eachmovie_randSplit1_data.txt eachmovie_randSplit1_testData.txt 1648 30000 10 .01 10 .001 /root/spark jarfile src/main/scala/results/ eachmovie_ALS_spark_results.txt"
    // sbt/sbt "run-main dmf.Run_stripped_ALS_Spark master data/ netflix_randSplit1_data.txt netflix_randSplit1_testData.txt 17770 480189 10 .01 10 sparkhome jar src/main/scala/results/ netflix_ALS_spark_results.txt"

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
    val big = options.getOrElse("big","false").toBoolean
    val first = options.getOrElse("first","false").toBoolean
    val m = options.getOrElse("m", "100").toInt
    val n = options.getOrElse("n", "100").toInt
    val repfact = options.getOrElse("repfact", "1").toInt

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
    println("first time    " + first) 
    println("m:            " + m)
    println("n:            " + n)
    println("repfact:          " + repfact)  

    // set up spark context ..
    // local[x] runs x threads
    val sc = new SparkContext(master,"BALS",sparkhome,List(jar))

    var trainData: spark.RDD[(Int,Int,Double)] = null

    // if(big && first){
    //   val newfile = trainfile + "_replicated"
    //   trainData = sc.textFile(trainfile,nsplits)
    //     .map(_.split(' '))
    //     .map{ elements => (elements(0).toInt-1,elements(1).toInt-1,elements(2).toDouble)}
    //     .flatMap(x => replicate(x,repfact,m,n) )

    //   trainData.map(x => (x._1 + " " + x._2 + " " + x._3)).saveAsTextFile(newfile)
    //   sc.stop()
    //   System.exit(0)
    // }
    // else { 
      if(big){
        trainData = sc.textFile(trainfile)
        .map(_.split(' '))
        .map{elements => (elements(0).toInt,elements(1).toInt,elements(2).toDouble)}
        //.cache
        .persist(StorageLevel.MEMORY_ONLY_SER)
      }
      else {
      trainData = sc.textFile(trainfile, nsplits)
        .map(_.split(' '))
        .map{elements => (elements(0).toInt-1,elements(1).toInt-1,elements(2).toDouble)}
        .cache//.persist(StorageLevel.MEMORY_ONLY_SER)
      }

    println("num partitions is: " + trainData.splits.size)

    // initialize W,H
    //val rand = new Random(seed)
    val W_array = Array.fill(m)(DoubleMatrix.rand(rank));
    val H_array = Array.fill(n)(DoubleMatrix.rand(rank));
    //val W = DenseMatrix.zeros[Double](m,rank);
    //val r = new Random(0)
    //val H = DenseMatrix.rand(n,rank,r);

    val WH = ALS(W_array,H_array,sc,trainData,m,n,rank,lambda,niter)
    
    sc.stop()
   }

   def ALS(W_array:Array[DoubleMatrix],H_array:Array[DoubleMatrix],sc:SparkContext,
      train_ratings:spark.RDD[(Int,Int,Double)], m:Int,n:Int, k:Int,lambda:Double, maxiter:Int)
       : (Array[DoubleMatrix],Array[DoubleMatrix]) = {

    val lambI = DoubleMatrix.eye(k).mul(lambda);
    //val lambI = DenseMatrix.eye[Double](k) :*= lambda;

    // arrays of dense vectors
    //var W_array = (0 to m-1).map{i => new DenseVector(W(i,::).iterator.map{case(k,v)=>v}.toArray)}.toArray
    //var H_array = (0 to n-1).map{i => new DenseVector(H(i,::).iterator.map{case(k,v)=>v}.toArray)}.toArray
    var W_b = sc.broadcast(W_array)
    var H_b = sc.broadcast(H_array)

    println("=================================")
    println("W_b SIZE is: " + W_b.value.size)
    println("MAX ROW: " + train_ratings.map{case (w,h,r)=>w}.reduce(Math.max))
    println("MAX COL: " + train_ratings.map{case (w,h,r)=>h}.reduce(Math.max))
    println("NNZ : " + train_ratings.count())
    val train_nnz = train_ratings.count()
    //val test_nnz = test_ratings.count()

    val starttime = System.currentTimeMillis

    for(iter <- 0 until maxiter){
      println("Iteration: " + iter)

      // update W matrix
      val temp_w = train_ratings
        .map({ case(w,h,r) => (w, (H_b.value(h).mmul(H_b.value(h).transpose()), H_b.value(h).mul(r))) })
        .reduceByKey{ case ((x1,y1), (x2,y2)) => (x1.add(x2),y1.add(y2))}
        .map { case (w, (xtx , xty)) => (w,Solve.solvePositive(xtx.add(lambI), xty))}.collect 
      
      temp_w.foreach{ case (w,v) => W_array(w)=v }

      // send out results
      W_b = sc.broadcast(W_array)

      // update H matrix
      val temp_h = train_ratings.map{
        case (w,h,r) => (h, (W_b.value(w).mmul(W_b.value(w).transpose()), W_b.value(w).mul(r))) }
        .reduceByKey{ case ((x1,y1), (x2,y2)) => (x1.add(x2),y1.add(y2))}
        .map { case (h, (xtx, xty)) => (h,Solve.solvePositive(xtx.add(lambI), xty))}.collect 
        
      temp_h.foreach{ case (h,v) => H_array(h)=v }

      // send out results
      H_b = sc.broadcast(H_array)
      val runtime = System.currentTimeMillis - starttime
      println("Runtime: " + runtime)
    }

    val final_runtime = System.currentTimeMillis - starttime
    println("Final Runtime: " + final_runtime)

    val tr_error = train_ratings.map{ case (w,h,r) => 
        val pred = W_b.value(w).dot(H_b.value(h)); (pred - r)*(pred - r)}.sum
    val tr_rel_err = Math.sqrt(tr_error)/Math.sqrt(train_nnz)
    println("tr_rel_err: " + tr_rel_err)

    return (W_array,H_array)
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

}
