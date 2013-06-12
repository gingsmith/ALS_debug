package als_debug

import scala.io.Source
import breeze.linalg._
import spark._
import java.io._
import scala.util._
import spark.SparkContext._
//import breeze.collection._

object DFC_ALS{


  // sbt/sbt "run-main dmf.DFC_ALS --datadir=data/ --train=eachmovie_randSplit1_data.txt --test=eachmovie_randSplit1_testData.txt --m=1648 --n=30000 --rank=10 --maxiter=10 --nsplits=4"
  // sbt/sbt "run-main dmf.DFC_ALS --datadir=data/ --train=movielens_1m_randSplit1_data.txt --test=movielens_1m_randSplit1_testData.txt --m=3952 --n=5000 --rank=10 --maxiter=10 --nsplits=4"
  // sbt/sbt "run-main dmf.DFC_ALS --datadir=data/ --train=movielens_10m_randSplit1_data.txt --test=movielens_10m_randSplit1_testData.txt --m=10677 --n=69876 --rank=10 --maxiter=10 --nsplits=4"
  // sbt/sbt "run-main dmf.DFC_ALS --datadir=data/ --train=netflix_randSplit1_data.txt --test=netflix_randSplit1_testData.txt --m=17770 --n=480188 --rank=10 --maxiter=10 --nsplits=4"


    // Read in file, create CSC matrix
  private def createCSCFromFile(m: Int, n: Int, filename: String) = {
    val builder = new CSCMatrix.Builder[Double](m, n)
    Source.fromFile(filename).getLines.map(_.split(' ')).map { elements =>
      (elements(0).toInt-1,elements(1).toInt-1,elements(2).toDouble)
    }.foreach { case(i,j,v) =>
      if(j < n){
        builder.add(i,j,v)
      }
      if(j % 10000 == 0) {
        println(j/10000)
      }
    }
    builder.result()
  }

  // get column of sparse matrix
  private def colSlice( A: CSCMatrix[Double], colIndex: Int ) : SparseVector[Double] = {
      val size = A.rows
      val rowStartIndex = A.colPtrs(colIndex)
      val rowEndIndex = A.colPtrs(colIndex + 1) - 1
      val capacity = rowEndIndex - rowStartIndex + 1
      val result = SparseVector.zeros[Double](size)
      result.reserve(capacity)
      var i = 0
      while( i < capacity ) {
         val thisindex = rowStartIndex + i
         val row = A.rowIndices(thisindex)
         val value = A.data(thisindex)
         result(row) = value
         i += 1
      }
      result
   }

  def main(args: Array[String]) {

    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "als_debug.CCDKryoRegistrator")
    System.setProperty("spark.storage.blockManagerHeartBeatMs", "120000")
    System.setProperty("spark.kryoserializer.buffer.mb", "1024")

    val options =  args.map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case Array(opt) => (opt -> "true")
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }.toMap

    val master = options.getOrElse("master", "local[4]")
    val datadir = options.getOrElse("datadir", "")
    val trainfile = options.getOrElse("train", "")
    val testfile = options.getOrElse("test", "")
    val rank = options.getOrElse("rank", "10").toInt
    val lambda = options.getOrElse("lambda", "0.01").toDouble
    val maxiter = options.getOrElse("maxiter", "5").toInt
    val jar = options.getOrElse("jars", "")
    val nsplits = options.getOrElse("nsplits", "4").toInt
    val sparkhome = options.getOrElse("sparkhome", System.getenv("SPARK_HOME"))
    val m = options.getOrElse("m", "100").toInt
    val n = options.getOrElse("n", "100").toInt

    println("master:       " + master)
    println("datadir:      " + datadir)
    println("train:    " + trainfile)
    println("test:     " + testfile)
    println("rank:         " + rank)
    println("lambda:       " + lambda)
    println("maxiter:        " + maxiter)
    println("jar:          " + jar)
    println("sparkhome:    " + sparkhome)
    println("nsplits:    " + nsplits)
    println("m:    " + m)
    println("n:    " + n)

    val Mmatrix = createCSCFromFile(m, n, datadir+trainfile)
    println("Filled Mmatrix")
    val Lmatrix = createCSCFromFile(m, n, datadir+testfile)
    println("Filled Lmatrix")

    val t_start = System.currentTimeMillis

    /* BEGIN Partition matrix & run MF */
    /**********************************************************/

    //Mmatrix:CSCMatrix[Double],m:Int,n:Int, k:Int,lambda:Double,maxiter:Int

    val partsize = n/nsplits
    println("about to make context")
    val sc = new SparkContext(master, "DFC_ALS", sparkhome, List(jar))
    println("made context")
    val M_array = (1 to Mmatrix.cols).map{ i => colSlice(Mmatrix,i-1)}
    println("mapped to array")
    val columnRdd = sc.parallelize(M_array, nsplits)
    println("parallelized array")
    println(columnRdd.count)
    val acc2 = columnRdd.mapPartitionsWithSplit{ case(i, columns) =>
      println("column to array")
      val colArray = columns.toArray
      println("making builder")
      val builder = new CSCMatrix.Builder[Double](m, colArray.size)
      println("made builder")
      colArray.zipWithIndex.foreach { case(column, j) =>
        if(j % 10000 == 0) {
          println(j/10000)
        }
        val myitr = column.array.iterator
        while(myitr.hasNext){
          val itr_val = myitr.next()
          builder.add(itr_val._1,j,itr_val._2)
        }
      }
      val localM = builder.result()
      Seq((i,als_base(localM,m,colArray.size,rank,lambda,maxiter))).toIterator
    }.collect().sortBy(_._1).map(_._2).toArray
    /**********************************************************/
    /* END MF partition */


    /* BEGIN Recombine partitions with MF projection */
    /**********************************************************/
    println("Running MF projection")
    val L_proj = mf_proj(acc2,n)
    val row_feats = L_proj._1
    val col_feats = L_proj._2

    /**********************************************************/
    /* END MF proj */

    val t_end=System.currentTimeMillis


    /* BEGIN Calculate & return final results */
    /**********************************************************/
    var tr_err=0.0
    val tr_itr = Mmatrix.activeIterator
    while(tr_itr.hasNext){
      val curr_val = tr_itr.next()
      val predval_tr = (row_feats(::,curr_val._1._1):*col_feats(::,curr_val._1._2)).sum
      val curr_err = (predval_tr - curr_val._2)
      tr_err = tr_err + curr_err*curr_err
    }
    tr_err=math.sqrt(tr_err)

    var te_err=0.0
    var part_te_err=0.0
    val te_itr = Lmatrix.activeIterator
    while(te_itr.hasNext){
      val curr_val = te_itr.next()

       val row = curr_val._1._1
       val col = curr_val._1._2
       val index = col/partsize
       val col_ind = col - index*partsize
       val part_predval_te = ((acc2(index)._1)(::,row):*(acc2(index)._2)(::,col_ind)).sum
       val part_curr_err = (part_predval_te - curr_val._2)
       part_te_err = part_te_err + part_curr_err*part_curr_err

      val predval_te = (row_feats(::,row):*col_feats(::,col)).sum
      val curr_err = (predval_te - curr_val._2)
      te_err = te_err + curr_err*curr_err
    }
    te_err=math.sqrt(te_err)
    part_te_err=math.sqrt(part_te_err)


    // print final error
    println("DFC complete!")
    println("Training Error: " + tr_err/math.sqrt(Mmatrix.activeSize))
    println("Partition-4splits Testing Error: " + part_te_err/math.sqrt(Lmatrix.activeSize))
    println("Projection-4splits Testing Error: " + te_err/math.sqrt(Lmatrix.activeSize))
    println("Elapsed Time without load: " + (t_end-t_start)/1000 + " seconds")
    /**********************************************************/
    /* END final results for DFC */

    val writer_stats = new PrintWriter(new File("DFC_" + trainfile + 
      "_nsplits" + nsplits + "_rank" + rank + "_maxiter" + maxiter+ ".log"))
      writer_stats.println("Training Error: " + tr_err/math.sqrt(Mmatrix.activeSize))
      writer_stats.println("Testing Error: " + te_err/math.sqrt(Lmatrix.activeSize))
      writer_stats.println("Elapsed Time without load: " + (t_end-t_start)/1000 + " seconds")
      writer_stats.close()

    sc.stop()
  }

  ////////// SGD BASE algorithm ////////////////
  def als_base(Mmatrix:CSCMatrix[Double],m:Int,n:Int, k:Int,lambda:Double,maxiter:Int)
       : (DenseMatrix[Double],DenseMatrix[Double]) = {

    // initialize W,H
    val W = DenseMatrix.zeros[Double](m,k);
    //val r = new Random(0)
    val H = DenseMatrix.rand(n,k);

    // initialize variables
    val lambI = DenseMatrix.eye[Double](k) :*= lambda;

    val rowscolsvals=Mmatrix.activeKeysIterator.map{k=>(k._1,k._2,Mmatrix(k._1,k._2))}.toArray

    // make sets of indices of columns
    val Hinds_map = rowscolsvals.groupBy(k=>k._1).mapValues(x=>x.map(c=>c._2).toList)
    // get size of each set
    val numHinds = Hinds_map.mapValues(x=>x.size)
    // make sets of indicies of rows
    val Winds_map = rowscolsvals.groupBy(k=>k._2).mapValues(x=>x.map(r=>r._1).toList)
    // get size of each set
    val numWinds = Winds_map.mapValues(x=>x.size)

    val Mdata_Hinds = rowscolsvals.groupBy(k=>k._1).mapValues(x=>x.map(v=>v._3).toArray)
    val Mvec_Hinds = Mdata_Hinds.mapValues(x=>new DenseVector(x))
    val Mdata_Winds = rowscolsvals.groupBy(k=>k._2).mapValues(x=>x.map(v=>v._3).toArray)
    val Mvec_Winds = Mdata_Winds.mapValues(x=>new DenseVector(x))

    // nonzero elements of M
    val M_nz = DenseVector(rowscolsvals.map(k=>k._3))

    println("Nonzero Elements for H and W calculated")

    for(iter <- 0 until maxiter){
      println("Iteration: " + iter)

      for(q<-0 until m){
        //println("Iteration: " + iter + "  Row: " + q)
        if(numHinds.contains(q)){
          val Hq = DenseMatrix.zeros[Double](numHinds(q),k)
          Hq(0 to numHinds(q)-1, 0 to k-1) := H(Hinds_map(q),0 to k-1)
          W(q,::) := (Hq.t*Hq+lambI)\(Hq.t*Mvec_Hinds(q))
       }
      }

      for(q<-0 until n){
        if(numWinds.contains(q)){
          val Wq = DenseMatrix.zeros[Double](numWinds(q),k)
          Wq(0 to numWinds(q)-1, 0 to k-1) := W(Winds_map(q),0 to k-1)
          H(q,::) := (Wq.t*Wq+lambI)\(Wq.t*Mvec_Winds(q))
        }
      }
    }

    println("W IS: ")
    println(W)
    println("H IS: ")
    println(H)
    return (W.t,H.t)
   }

  ////////////////// MF projection ///////////////////
  def mf_proj(acc:Array[(DenseMatrix[Double],DenseMatrix[Double])], numcols:Int) : (DenseMatrix[Double],DenseMatrix[Double]) = {
    // project onto first partition ...
    val totalparts = acc.length // get the number of partitions
    val partsize = numcols/totalparts

    val C = acc.head // get first element
    val X_A = C._1 // set output A to C.A

    val C_pinv_A = pinv((C._2).t)
    val C_pinv_B = pinv((C._1).t)
    val k = X_A.rows
    val X_B = DenseMatrix.zeros[Double](k,numcols)
    X_B(::,0 to partsize-1):=C._2 // set initial portion to first partition's B
    // iterate through the rest of the list ..
    acc.tail.zipWithIndex.foreach{ case(w,i) =>
      val M_hat_B = ((C._2*(C_pinv_A.t))*(C_pinv_B*(w._1).t))*w._2
      val curr_ind = (i+1)*partsize
      X_B(::, curr_ind to curr_ind+partsize-1):=M_hat_B
    }

    return (X_A,X_B)
  }
}