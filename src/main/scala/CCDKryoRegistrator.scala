package als_debug

import com.esotericsoftware.kryo.Kryo
import spark.KryoRegistrator
import breeze.linalg._

class CCDKryoRegistrator extends KryoRegistrator {

  def registerClasses(kryo: Kryo) {
    // This avoids a large number of hash table lookups.
    kryo.setReferences(false)
  }

  private def registerClass[VD: Manifest, ED: Manifest, VD2: Manifest](kryo: Kryo) {
    // Register all the types used either in a broadcast or in an RDD
    kryo.register(classOf[(Int, Int, Double)])
    kryo.register(classOf[(Int, Array[Double])])
    kryo.register(classOf[(Array[Double],Array[Double])])
    kryo.register(classOf[(Int, (Array[Double],Array[Double]))])
    kryo.register(classOf[DenseVector[Double]])
    kryo.register(classOf[Array[DenseVector[Double]]])
    kryo.register(classOf[(Int, Int, (Double, Double))])
  }
}