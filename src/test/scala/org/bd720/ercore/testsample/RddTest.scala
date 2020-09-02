package org.bd720.ercore.testsample
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import org.bd720.ercore.common.SparkEnvSetup
class RddTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "reduce" in {
    val rdd = spark.sparkContext.parallelize(Seq(("a", 1), ("b", 1), ("c", 2), ("a", 1), ("a", 2)))
    assert(rdd.count() == 5)
    val f = (a: (String, Int), b: (String, Int)) => (a._1 + b._1, a._2 + b._2)
    val result: (String, Int) = rdd.reduce(f)
    assertResult(7)(result._2)
    val rdd2: RDD[(String, Int)] = rdd.reduceByKey((x, y) => x + y)
    assertResult(("a", 4))(rdd2.first())
    val otherRdd = spark.sparkContext.parallelize(Seq(("a", "first"), ("b", "Second"), ("d", "missing value")))
    val joint = rdd.leftOuterJoin(otherRdd)
    joint.foreach(x=>println("join result "+x))
  }
}
