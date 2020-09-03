package org.bd720.ercore.methods.blockrefinement
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.methods.datastructure.{BlockAbstract, BlockClean, BlockDirty}
class BlockPurgingTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "sumPrecedentLevels v1" in {
    var input = Array[(Double, (Double, Double))]()
    input +:= (0.8, (0.1, 0.2))
    input +:= (0.5, (0.21, 0.2))
    input +:= (0.9, (0.71, 0.1))
    val result = BlockPurging.sumPrecedentLevels(input)
    result.foreach(x => println("result=" + x))
    assert(3 == result.size)
    val leftItem = result.last
    assertResult((0.8, (1.02, 0.5)))(leftItem)
  }
  it should "sumPrecedentLevels v2" in {
    var input = Array[(Double, (Double, Double))]()
    input +:= (5.0, (1.0, 20.0))
    input +:= (0.5, (10.0, 2.0))
    input +:= (0.9, (100.0, 0.2))
    val result = BlockPurging.sumPrecedentLevels(input)
    result.foreach(x => println("result=" + x))
    assert(3 == result.size)
    val leftItem = result.last
    assertResult((5.0, (111.0, 22.2)))(leftItem)
  }
  it should "calcMaxComparisonNumber v1" in {
    var input = Array[(Double, (Double, Double))]()
    input +:= (9.0, (9.0, 6.0))
    input +:= (4.0, (4.0, 4.0))
    var max = BlockPurging.calcMaxComparisonNumber(input, smoothFactor = 1.0)
    assert(9.0 == max)
    max = BlockPurging.calcMaxComparisonNumber(input, smoothFactor = 0.5)
    assert(9.0 == max)
    max = BlockPurging.calcMaxComparisonNumber(input, smoothFactor = 0.1)
    assert(9.0 == max)
    max = BlockPurging.calcMaxComparisonNumber(input, smoothFactor = 0.0000001)
    assert(9.0 == max)
  }
  it should "calcMaxComparisonNumber v2" in {
    var input = Array[(Double, (Double, Double))]()
    input +:= (9.0, (9.0, 6.0)) 
    input +:= (4.0, (4.0, 4.0)) 
    input +:= (12.0, (12.0, 7.0)) 
    input +:= (16.0, (16.0, 8.0)) 
    input +:= (2.0, (2.0, 3.0)) 
    var max = BlockPurging.calcMaxComparisonNumber(input, smoothFactor = 1.0)
    assert(4.0 == max)
    max = BlockPurging.calcMaxComparisonNumber(input, smoothFactor = 0.8)
    assert(4.0 == max)
    max = BlockPurging.calcMaxComparisonNumber(input, smoothFactor = 0.7)
    assert(4.0 == max)
    max = BlockPurging.calcMaxComparisonNumber(input, smoothFactor = 0.5)
    assert(16.0 == max)
    max = BlockPurging.calcMaxComparisonNumber(input, smoothFactor = 0.1)
    assert(16.0 == max)
    max = BlockPurging.calcMaxComparisonNumber(input, smoothFactor = 0.0000001)
    assert(16.0 == max)
  }
  it should "calcMaxComparisonNumber v3" in {
    var input = Array[(Double, (Double, Double))]()
    input +:= (0.8, (0.1, 0.2))
    input +:= (0.5, (0.2, 0.2))
    input +:= (0.9, (0.7, 0.1))
    input +:= (0.1, (0.7, 0.1))
    val smoothFactor = 0.5
    val max = BlockPurging.calcMaxComparisonNumber(input, smoothFactor)
    assert(0.5 == max)
  }
  it should "blockPurging v1" in {
    val baRdd: RDD[BlockAbstract] = spark.sparkContext.parallelize(
      Seq(BlockDirty(99, Array[Set[Int]](Set[Int](11, 12, 15, 16)), 0.91, 5555))
    )
    val bapRdd = BlockPurging.blockPurging(baRdd, 0.5)
    assert(0 == bapRdd.count())
    bapRdd.foreach(x => println("after-purging:" + x))
  }
  it should "blockPurging v2" in {
    val baRdd: RDD[BlockAbstract] = spark.sparkContext.parallelize(
      Seq(
        BlockDirty(99, Array[Set[Int]](Set[Int](11, 12, 15, 16)), 0.91, 5555),
        BlockDirty(100, Array[Set[Int]](Set[Int](11, 12, 15, 16)), 0.81, 5555)
      )
    )
    val bapRdd = BlockPurging.blockPurging(baRdd, 0.5)
    assert(0 == bapRdd.count())
    bapRdd.foreach(x => println("after-purging:" + x))
  }
  it should "blockPurging v3" in {
    val baRdd: RDD[BlockAbstract] = spark.sparkContext.parallelize(
      Seq(
        BlockClean(99, Array[Set[Int]](Set[Int](1, 3, 5), Set[Int](2, 4, 6)), 0.91, 5555),
        BlockClean(100, Array[Set[Int]](Set[Int](1, 3, 5), Set[Int](2, 4, 6)), 0.81, 666)
      )
    )
    val bapRdd = BlockPurging.blockPurging(baRdd, 0.5)
    assert(0 == bapRdd.count())
    bapRdd.foreach(x => println("after-purging:" + x))
  }
  it should "blockPurging v4" in {
    val baRdd: RDD[BlockAbstract] = spark.sparkContext.parallelize(
      Seq(
        BlockClean(99, Array[Set[Int]](Set[Int](1, 3, 5), Set[Int](2, 4, 6)), 0.91, 5555), 
        BlockClean(100, Array[Set[Int]](Set[Int](1, 3, 5), Set[Int](2, 4, 6)), 0.81, 666), 
        BlockClean(101, Array[Set[Int]](Set[Int](1, 3, 5, 6), Set[Int](4, 6, 8)), 0.52, 77), 
        BlockClean(102, Array[Set[Int]](Set[Int](1, 6), Set[Int](8, 10, 12)), 0.65, 77) 
      )
    )
    val bapRdd = BlockPurging.blockPurging(baRdd, 0.5)
    bapRdd.foreach(x => println("after-purging:" + x))
    assert(3 == bapRdd.count())
  }
  it should "blockPurging v5" in {
    val baRdd: RDD[BlockAbstract] = spark.sparkContext.parallelize(
      Seq(
        BlockClean(99, Array[Set[Int]](Set[Int](1, 3, 5), Set[Int](2, 4, 6)), 0.91, 5555), 
        BlockClean(100, Array[Set[Int]](Set[Int](1, 3, 5), Set[Int](2, 4, 6)), 0.91, 666), 
        BlockClean(101, Array[Set[Int]](Set[Int](1, 3, 5, 6), Set[Int](4, 6, 8)), 0.91, 77), 
        BlockClean(102, Array[Set[Int]](Set[Int](1, 6), Set[Int](8, 10, 12)), 0.91, 77) 
      )
    )
    val bapRdd = BlockPurging.blockPurging(baRdd, 0.9)
    bapRdd.foreach(x => println("after-purging:" + x))
    assert(3 == bapRdd.count())
  }
}
