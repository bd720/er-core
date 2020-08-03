package org.wumiguo.ser.methods.util
import org.apache.spark.rdd.RDD
import org.wumiguo.ser.methods.blockbuilding.TokenBlocking
import org.wumiguo.ser.methods.datastructure._
object Converters {
  def blocksToProfileBlocks(blocks: RDD[BlockAbstract]): RDD[ProfileBlocks] = {
    blocks
      .flatMap(blockIDProfileIDFromBlock)
      .groupByKey()
      .map(x => ProfileBlocks(x._1, x._2.toSet))
      .cache() // <--- GM
  }
  def blockIDProfileIDFromBlock(block: BlockAbstract): Iterable[(Int, BlockWithComparisonSize)] = {
    val blockWithComparisonSize = BlockWithComparisonSize(block.blockID, block.getComparisonSize())
    block.getAllProfiles.map((_, blockWithComparisonSize))
  }
  def profilesBlockToBlocks(profilesBlocks: RDD[ProfileBlocks], separatorIDs: Array[Int] = Array.emptyIntArray): RDD[BlockAbstract] = {
    val blockIDProfileID = profilesBlocks flatMap {
      profileWithBlocks =>
        val profileID = profileWithBlocks.profileID
        profileWithBlocks.blocks map {
          BlockWithSize =>
            (BlockWithSize.blockID, profileID)
        }
    }
    val blocks = blockIDProfileID.groupByKey().map {
      block =>
        val blockID = block._1
        val profilesID = block._2.toSet
        if (separatorIDs.isEmpty) {
          BlockDirty(blockID, Array(profilesID))
        }
        else {
          BlockClean(blockID, TokenBlocking.separateProfiles(profilesID, separatorIDs))
        }
    }
    blocks.filter(_.getComparisonSize() > 0).map(x => x)
  }
}
