package org.bd720.ercore.methods.blockrefinement
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.datastructure.ProfileBlocks
object BlockFiltering {
  def blockFiltering(profilesWithBlocks: RDD[ProfileBlocks], ratio: Double): RDD[ProfileBlocks] = {
    profilesWithBlocks map {
      profileWithBlocks =>
        val blocksSortedByComparisons = profileWithBlocks.blocks.toList.sortWith(_.comparisons < _.comparisons)
        val blocksToKeep = Math.round(blocksSortedByComparisons.size * ratio).toInt
        ProfileBlocks(profileWithBlocks.profileID, blocksSortedByComparisons.take(blocksToKeep).toSet)
    }
  }
  def blockFiltering(profilesWithBlocks: RDD[ProfileBlocks], ratio: Double, minCardinality: Int = 1): RDD[ProfileBlocks] = {
    profilesWithBlocks map {
      profileWithBlocks =>
        val blocksSortedByComparisons = profileWithBlocks.blocks.toList.sortWith(_.comparisons < _.comparisons)
        val blocksToKeep = Math.round(blocksSortedByComparisons.size * ratio).toInt
        ProfileBlocks(profileWithBlocks.profileID, blocksSortedByComparisons.take(blocksToKeep).toSet)
    }
  }
  def blockFilteringAdvanced(profilesWithBlocks: RDD[ProfileBlocks], r: Double, minCardinality: Int = 1): RDD[ProfileBlocks] = {
    profilesWithBlocks map {
      profileWithBlocks =>
        val blocksSortedByComparisons = profileWithBlocks.blocks.toList.sortWith(_.comparisons < _.comparisons)
        val blocksToKeep = Math.round(blocksSortedByComparisons.size * r).toInt
        val threshold = blocksSortedByComparisons(blocksToKeep - 1).comparisons
        ProfileBlocks(profileWithBlocks.profileID, blocksSortedByComparisons.filter(_.comparisons <= threshold).toSet)
    }
  }
}
