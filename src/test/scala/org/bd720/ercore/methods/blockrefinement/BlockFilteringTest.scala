package org.wumiguo.ser.methods.blockrefinement
import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.datastructure.{BlockWithComparisonSize, ProfileBlocks}
class BlockFilteringTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(this.getClass.getName)
  it should "filtering block V1 " in {
    val profileBlocks = spark.sparkContext.parallelize(Seq(
      ProfileBlocks(1, Set(BlockWithComparisonSize(91, 12.0), BlockWithComparisonSize(11, 2.0), BlockWithComparisonSize(21, 6.0), BlockWithComparisonSize(111, 4.0))),
      ProfileBlocks(2, Set(BlockWithComparisonSize(92, 20.0), BlockWithComparisonSize(12, 6.0))),
      ProfileBlocks(3, Set(BlockWithComparisonSize(930, 56.0), BlockWithComparisonSize(13, 6.0))),
      ProfileBlocks(4, Set(BlockWithComparisonSize(940, 72.0), BlockWithComparisonSize(41, 6.0))),
      ProfileBlocks(5, Set(BlockWithComparisonSize(95, 72.0), BlockWithComparisonSize(551, 6.0), BlockWithComparisonSize(155, 12.0)))
    ))
    val filtered = BlockFiltering.blockFiltering(profileBlocks, 0.5).sortBy(_.profileID)
    assert(5 == filtered.count(), "block filtering does not reduce profile block number")
    println("count = " + filtered.count())
    val first = filtered.first()
    println("first = " + first)
    assert(1 == first.profileID)
    assertResult(Set(BlockWithComparisonSize(11, 2.0), BlockWithComparisonSize(111, 4.0)))(first.blocks)
  }
  it should "filtering block V2 " in {
    val profileBlocks = spark.sparkContext.parallelize(Seq(
      ProfileBlocks(1, Set(BlockWithComparisonSize(91, 12.0), BlockWithComparisonSize(11, 2.0), BlockWithComparisonSize(21, 6.0), BlockWithComparisonSize(111, 4.0))),
      ProfileBlocks(2, Set(BlockWithComparisonSize(92, 20.0), BlockWithComparisonSize(12, 6.0))),
      ProfileBlocks(3, Set(BlockWithComparisonSize(930, 56.0), BlockWithComparisonSize(13, 6.0))),
      ProfileBlocks(4, Set(BlockWithComparisonSize(940, 72.0), BlockWithComparisonSize(41, 6.0))),
      ProfileBlocks(5, Set(BlockWithComparisonSize(95, 72.0), BlockWithComparisonSize(551, 6.0), BlockWithComparisonSize(155, 12.0)))
    ))
    val filtered = BlockFiltering.blockFiltering(profileBlocks, 0.75).sortBy(_.profileID)
    assert(5 == filtered.count(), "block filtering does not reduce profile block number")
    println("count = " + filtered.count())
    val first = filtered.first()
    println("first = " + first)
    assert(1 == first.profileID)
    assertResult(Set(BlockWithComparisonSize(11, 2.0), BlockWithComparisonSize(111, 4.0), BlockWithComparisonSize(21, 6.0)))(first.blocks)
  }
  it should "filtering block V3 " in {
    val profileBlocks = spark.sparkContext.parallelize(Seq(
      ProfileBlocks(1, Set(BlockWithComparisonSize(91, 12.0), BlockWithComparisonSize(11, 2.0), BlockWithComparisonSize(21, 6.0), BlockWithComparisonSize(111, 4.0))),
      ProfileBlocks(2, Set(BlockWithComparisonSize(92, 20.0), BlockWithComparisonSize(12, 6.0))),
      ProfileBlocks(3, Set(BlockWithComparisonSize(930, 56.0), BlockWithComparisonSize(13, 6.0))),
      ProfileBlocks(4, Set(BlockWithComparisonSize(940, 72.0), BlockWithComparisonSize(41, 6.0))),
      ProfileBlocks(5, Set(BlockWithComparisonSize(95, 72.0), BlockWithComparisonSize(551, 6.0), BlockWithComparisonSize(155, 12.0)))
    ))
    val filtered = BlockFiltering.blockFiltering(profileBlocks, 1.0).sortBy(_.profileID)
    assert(5 == filtered.count(), "block filtering does not reduce profile block number")
    println("count = " + filtered.count())
    val first = filtered.first()
    println("first = " + first)
    assert(1 == first.profileID)
    assertResult(Set(BlockWithComparisonSize(11, 2.0), BlockWithComparisonSize(111, 4.0), BlockWithComparisonSize(21, 6.0), BlockWithComparisonSize(91, 12.0)))(first.blocks)
  }
}
