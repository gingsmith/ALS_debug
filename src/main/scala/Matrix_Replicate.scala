package als_debug

import java.util.Arrays
import scala.io.Source
//import breeze.linalg._
import spark._
import spark.storage.StorageLevel
import java.io._
import scala.util._
// need _ to include everything in package
// reduceByKey in implicit RDD cast
import spark.SparkContext._


object Matrix_Replicate {

	// replicates a matrix by "repfact" in each dimension
	// saves the matrix as the original one with '_replicated' appended 

	def main(args: Array[String]) {

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
	    val jar = options.getOrElse("jars", "")
	    val nsplits = options.getOrElse("nsplits", "4").toInt
	    val sparkhome = options.getOrElse("sparkhome", System.getenv("SPARK_HOME"))
	    val m = options.getOrElse("m", "100").toInt
	    val n = options.getOrElse("n", "100").toInt
	    val repfact = options.getOrElse("repfact", "1").toInt

	   	// print out input
	    println("master:       " + master)
	    println("train:        " + trainfile)
	    println("jar:          " + jar)
	    println("sparkhome:    " + sparkhome)
	    println("nsplits:      " + nsplits) 
	    println("m:            " + m)
	    println("n:            " + n)
	    println("repfact:          " + repfact)  

    	val sc = new SparkContext(master,"Matrix_Replicate",sparkhome,List(jar))
		val newfile = trainfile + "_replicated"
		val trainData = sc.textFile(trainfile,nsplits)
		    .map(_.split(' '))
		    .map{ elements => (elements(0).toInt-1,elements(1).toInt-1,elements(2).toDouble)}
		    .flatMap(x => replicate(x,repfact,m,n) )

		trainData.map(x => (x._1 + " " + x._2 + " " + x._3)).saveAsTextFile(newfile)
		sc.stop()
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