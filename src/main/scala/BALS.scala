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

    if (args.length < 13) {
      System.err.println("Usage: Run_ALS_Spark <master> <datadir> <trainfile> <m> <n> <rank> <lambda> <maxiter> <sparkhome> <jar> <outdir> <outfile>")
      System.exit(1)
    }

    // print out input
    println("Master:      " + args(0))
    println("Data Dir:    " + args(1)); println("Train file:       " + args(2));
    println("m:           " + args(3)); println("n:                " + args(4)); 
    println("rank:        " + args(5)); println("lambda:           " + args(6)); println("max iter:         " + args(7));
    println("spark home:   " + args(8)); println("jar:   " + args(9));
    println("outdir: " + args(10)); println("outfile: " + args(11));

    // read in input
    val master=args(0); val datadir=args(1); val trainfile = args(2);
    val m=args(3).toInt; val n=args(4).toInt;
    val rank=args(5).toInt; val lambda=args(6).toDouble; val maxiter=args(7).toInt;
    val sparkhome = args(8); val jar = args(9);
    val outdir = args(10); val outfile = args(11);

    // set up spark context ..
    // local[x] runs x threads
    val sc = new SparkContext(master,"BALS",sparkhome, List(jar))

    val train_ratings: spark.RDD[(Int,Int,Double)] = sc.textFile(datadir+trainfile)
      .map(_.split(' ')).map{ elements => (elements(0).toInt-1,elements(1).toInt-1,elements(2).toDouble)}.persist(StorageLevel.MEMORY_ONLY_SER)
    //val test_ratings: spark.RDD[(Int,Int,Double)] = sc.textFile(datadir+testfile)
    //  .map(_.split(' ')).map{ elements => (elements(0).toInt-1,elements(1).toInt-1,elements(2).toDouble)}.persist(StorageLevel.MEMORY_ONLY_SER)

    // initialize W,H
    val W = DenseMatrix.zeros[Double](m,rank);
    val r = new Random(0)
    val H = DenseMatrix.rand(n,rank,r);

    val WH = ALS(W,H,sc,train_ratings,m,n,
      rank,lambda,maxiter,outdir,outfile)
    
    sc.stop()
   }

   def ALS(W:DenseMatrix[Double],H:DenseMatrix[Double],sc:SparkContext,
      train_ratings:spark.RDD[(Int,Int,Double)],
      m:Int,n:Int, k:Int,lambda:Double, maxiter:Int,outdir:String,outfile:String)
       : (DenseMatrix[Double],DenseMatrix[Double]) = {

    val lambI = DenseMatrix.eye[Double](k) :*= lambda;

    // arrays of dense vectors
    var W_array = (0 to m-1).map{i => new DenseVector(W(i,::).iterator.map{case(k,v)=>v}.toArray)}.toArray
    var H_array = (0 to n-1).map{i => new DenseVector(H(i,::).iterator.map{case(k,v)=>v}.toArray)}.toArray
    var W_b = sc.broadcast(W_array)
    var H_b = sc.broadcast(H_array)

    var times = new Array[Double](maxiter)

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
        .map({ case(w,h,r) => (w, (H_b.value(h)*(H_b.value(h).t), H_b.value(h)*r)) })
        .reduceByKey{ case ((x1,y1), (x2,y2)) => (x1 + x2,y1 + y2)}
        .map { case (w, (xtx , xty)) => (w,(xtx + lambI)\(xty))}.collect 
      
      temp_w.foreach{ case (w,v) => W_array(w)=v }

      // send out results
      W_b = sc.broadcast(W_array)

      // update H matrix
      val temp_h = train_ratings.map{
        case (w,h,r) => (h, (W_b.value(w)*(W_b.value(w).t), W_b.value(w)*r)) }
        .reduceByKey{ case ((x1,y1), (x2,y2)) => (x1 + x2,y1 + y2)}
        .map { case (h, (xtx, xty)) => (h,(xtx + lambI)\(xty))}.collect 
      temp_h.foreach{ case (h,v) => H_array(h)=v }

      // send out results
      H_b = sc.broadcast(H_array)
      val runtime = System.currentTimeMillis - starttime
      println("Runtime: " + runtime)
      times(iter) = runtime
    }

    val writer = new PrintWriter(new File(outdir+outfile))
    for(i<-0 until maxiter){
      writer.println(times(i))
    }
    writer.flush()
    writer.close()

    val final_runtime = System.currentTimeMillis - starttime
    println("Final Runtime: " + final_runtime)

    val tr_error = train_ratings.map{ case (w,h,r) => 
        val pred = W_b.value(w).dot(H_b.value(h)); (pred - r)*(pred - r)}.sum
    val tr_rel_err = Math.sqrt(tr_error)/Math.sqrt(train_nnz)
    println("tr_rel_err: " + tr_rel_err)

    return (W,H)
   }
}
