package org.bd720.ercore.methods.util
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.methods.datastructure.{BlockAbstract, BlockClean, BlockDirty, BlockWithComparisonSize, ProfileBlocks}
class ConvertersTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(this.getClass.getName)
  it should "blockIDProfileIDFromBlock v1" in {
    val aBlock: BlockAbstract = BlockDirty(99, Array[Set[Int]](Set[Int](11, 12, 15)), 0.91, 5555)
    val profileBlocks = Converters.blockIDProfileIDFromBlock(aBlock)
    profileBlocks.foreach(x => println("bwc:" + x))
    val first = profileBlocks.toList.sortBy(_._1).head
    assertResult((11, BlockWithComparisonSize(99, 6.0)))(first)
    assertResult((11, BlockWithComparisonSize(aBlock.blockID, aBlock.getComparisonSize())))(first)
  }
  it should "blockIDProfileIDFromBlock v2" in {
    val block1: BlockAbstract = BlockClean(99, Array[Set[Int]](Set[Int](11, 21, 31)), 0.91, 5555)
    val pb1Rdd = Converters.blockIDProfileIDFromBlock(block1)
    pb1Rdd.foreach(x => println("bwc:" + x))
    val pb1 = pb1Rdd.toList.sortBy(_._1).head
    assertResult((11, BlockWithComparisonSize(99, 0.0)))(pb1)
    val block2: BlockAbstract = BlockClean(99, Array[Set[Int]](Set[Int](11, 21, 31), Set[Int](12, 22, 32)), 0.91, 5555)
    val pb2Rdd = Converters.blockIDProfileIDFromBlock(block2)
    pb2Rdd.foreach(x => println("bwc:" + x))
    val pb2 = pb2Rdd.toList.sortBy(_._1).head
    assertResult((11, BlockWithComparisonSize(99, 9.0)))(pb2)
  }
  it should "blocksToProfileBlocks v1" in {
    val blocks: RDD[BlockAbstract] = spark.sparkContext.parallelize(Seq(BlockDirty(12, Array[Set[Int]](Set[Int](1, 3, 5)), 0.91, 5555)))
    val profileBlocks = Converters.blocksToProfileBlocks(blocks)
    profileBlocks.foreach(x => println("pb:" + x))
    val sorted = profileBlocks.sortBy(_.profileID)
    assertResult(ProfileBlocks(1, Set(BlockWithComparisonSize(12, 6.0))))(sorted.first())
  }
  it should "blocksToProfileBlocks v2" in {
    val blocks: RDD[BlockAbstract] = spark.sparkContext.parallelize(Seq(BlockClean(12, Array[Set[Int]](Set[Int](1, 3, 5)), 0.91, 5555)))
    val profileBlocks = Converters.blocksToProfileBlocks(blocks)
    profileBlocks.foreach(x => println("pb:" + x))
    val sorted = profileBlocks.sortBy(_.profileID)
    assertResult(ProfileBlocks(1, Set(BlockWithComparisonSize(12, 0.0))))(sorted.first())
  }
  it should "blocksToProfileBlocks v3" in {
    val blocks: RDD[BlockAbstract] = spark.sparkContext.parallelize(Seq(BlockClean(12, Array[Set[Int]](Set[Int](1, 3, 5), Set[Int](2, 4, 5)), 0.91, 5555)))
    val profileBlocks = Converters.blocksToProfileBlocks(blocks)
    profileBlocks.foreach(x => println("pb:" + x))
    val sorted = profileBlocks.sortBy(_.profileID)
    assertResult(ProfileBlocks(1, Set(BlockWithComparisonSize(12, 9.0))))(sorted.first())
  }
}
