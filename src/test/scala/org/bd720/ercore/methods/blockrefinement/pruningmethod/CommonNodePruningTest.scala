package org.wumiguo.ser.methods.blockrefinement.pruningmethod
import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.blockrefinement.pruningmethod.PruningUtils.WeightTypes
import org.wumiguo.ser.methods.datastructure.{BlockWithComparisonSize, ProfileBlocks}
class CommonNodePruningTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "calcWeights" in {
    val pb = ProfileBlocks(1083, Set(BlockWithComparisonSize(140, 56.0), BlockWithComparisonSize(331, 380.0), BlockWithComparisonSize(3403, 272.0)))
    val weights = Array[Double](0.5, 0.6, 0.4)
    val neighbours = Array[Int](2, 4, 8, 10)
    val ents = Array[Double](1.0, 0.8, 0.6)
    val neighboursNumber = 4
    val blockIndex = spark.sparkContext.broadcast(Map((1,Array[Set[Int]](Set[Int](1,3,5)))))
    val separatorIDs = Array[Int](2)
  }
}
