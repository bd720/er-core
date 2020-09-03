package org.bd720.ercore.methods.blockrefinement.pruningmethod
import org.scalatest.FlatSpec
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.methods.datastructure.{BlockWithComparisonSize, ProfileBlocks}
class CEPTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "calcFreq" in {
    val weights = Array[Double](2.1, 2.2, 2.3, 2.1, 2.5, 2.6)
    val neighbors = Array[Int](0, 2, 3, 4)
    val neighborsNumber: Int = 3 
    val arr = CEP.calcFreq(weights, neighbors, neighborsNumber)
    assertResult(List[(Double, Double)]((2.3, 1.0), (2.1, 2.0)))(arr.toList)
  }
  it should "calcThreshold " in {
    val pbRdd = spark.sparkContext.makeRDD(Seq(
      ProfileBlocks(1083, Set(BlockWithComparisonSize(140, 56.0), BlockWithComparisonSize(331, 380.0), BlockWithComparisonSize(3403, 272.0))),
      ProfileBlocks(1084, Set(BlockWithComparisonSize(373, 188790.0), BlockWithComparisonSize(2852, 930.0), BlockWithComparisonSize(3134, 90.0), BlockWithComparisonSize(1648, 701406.0))),
      ProfileBlocks(1085, Set(BlockWithComparisonSize(2248, 156.0), BlockWithComparisonSize(985, 5260142.0), BlockWithComparisonSize(966, 930.0)))
    ))
  }
  it should "CEP " in {
  }
}
