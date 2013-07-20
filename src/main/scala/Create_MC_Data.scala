package als_debug

import java.util.Arrays
import scala.io.Source
import spark._
import spark.storage.StorageLevel
import scala.util._
import org.jblas._
import spark.SparkContext._


object Create_MC_Data {

	/* 
	 * Code used to create synthetic matrix completion data.
	 * Inputs:
	 *    m - number of rows
	 *    n - number of columns 
     *    rank - the underlying rank 
     *    train_sampfact - oversampling factor
     *    noise - whether to add gaussian noise to training data
     *    sigma - std deviation to use for adding noise
     *    trainfile - name for output train data file
     *	  test - whether to calculate test data
     *    test_sampfact - percentage of training data to save as test data
     * 	  testfile - name for output test data file
	 * Outputs:
	 *    training data - 
	 *	  (optionally) testing data
	*/

    // example:
    // sbt/sbt "run-main als_debug.Create_MC_Data "

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
	    val jar = options.getOrElse("jars", "")
	    val sparkhome = options.getOrElse("sparkhome", System.getenv("SPARK_HOME"))
	    val m = options.getOrElse("m", "100").toInt
	    val n = options.getOrElse("n", "100").toInt
        val rank = options.getOrElse("rank", "10").toInt
        val train_sampfact = options.getOrElse("train_sampfact", "1.0").toDouble
        val sigma = options.getOrElse("sigma", "0.1").toDouble
        val noise = options.getOrElse("noise","false").toBoolean
        val trainfile = options.getOrElse("trainfile", "synthetic_mc_trainData")
        val test = options.getOrElse("test", "false").toBoolean
        val test_sampfact = options.getOrElse("test_sampfact", "0.1").toDouble
        val testfile = options.getOrElse("testfile", "synthetic_mc_testData")

	   	// print out input
	    println("master:        " + master)
	    println("jar:           " + jar)
	    println("sparkhome:     " + sparkhome)
	    println("m:             " + m)
	    println("n:             " + n)
        println("rank           " + rank)
        println("train_sampfact " + train_sampfact)
        println("sigma          " + sigma)
        println("with noise     " + noise)
        println("trainfile      " + trainfile)
        println("test 		    " + test)
        println("test_sampfact  " + test_sampfact)
        println("testfile		" + testfile)

		val sc = new SparkContext(master,"Create_MC_Data",sparkhome,List(jar))

        // make factored matrices
        val A = DoubleMatrix.randn(m,rank)
        val B = DoubleMatrix.randn(rank,n)

        // normalize (optional)
        val z = 1/(Math.sqrt(Math.sqrt(rank)))
        A.mmuli(z)
        B.mmuli(z)

        // multiply to get test matrix
        val testData = A.mmul(B)

        // find degrees of freedom
        val df = rank*(m+n-rank)

        // calculate # of entries to sample
        val sampsize = Math.min(Math.round(train_sampfact*df), Math.round(.99*m*n)).toInt

        // randomly sample entries without replacement
        val rand = new Random()
        val mn = m*n

        // create shuffled list of indices
        val shuffled = rand.shuffle(1 to mn toIterable)

        // get first sampsize elements
        val omega = shuffled.slice(0,sampsize)

        // order elements -- will save in column-major format
        val ordered = omega.sortWith(_ < _).toArray

        // put in sparse data format
        val trainData = sc.parallelize(ordered)
        		.map(x=> (testData.indexRows(x),testData.indexColumns(x),testData.get(x)))

        // optionally add gaussian noise and save training data
        if(noise){
        	trainData
        		.map(x => (x._1 + " " + x._2 + " " + (x._3+rand.nextGaussian*sigma)))
        		.saveAsTextFile(trainfile)
        }
        else{
			trainData
        		.map(x => (x._1 + " " + x._2 + " " + x._3))
        		.saveAsTextFile(trainfile)
        }

        // optionally generate testing data as well
        if(test){

        	// calculate test sample size
        	val test_sampsize = Math
        		.min(Math.round(sampsize*test_sampfact),Math.round(mn-sampsize))
        		.toInt

        	// get test sample
        	val test_omega = shuffled.slice(sampsize,sampsize+test_sampsize)

        	// order elements -- will save in column-major format
        	val test_ordered = test_omega.sortWith(_ < _).toArray

        	// put in sparse data format
        	val out_testData = sc.parallelize(test_ordered)
        		.map(x=> (testData.indexRows(x),testData.indexColumns(x),testData.get(x)))

        	// save testing data
        	out_testData
        		.map(x => (x._1 + " " + x._2 + " " + x._3))
        		.saveAsTextFile(testfile)
        }
        
		sc.stop()
	}

}