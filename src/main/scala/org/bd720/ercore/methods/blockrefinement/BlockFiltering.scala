package org.wumiguo.ser.methods.blockrefinement
import org.apache.spark.rdd.RDD
import org.wumiguo.ser.methods.datastructure.ProfileBlocks
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
      /*val blocksNumberToKeep = math.max(Math.round(profileWithBlocks.blocks.size * r).toInt, minCardinality)//Calculates the number of blocks to keep
      val blocksToKeep = new BoundedPriorityQueue[BlockWithComparisonSize](blocksNumberToKeep) //Creates a new priority queue of the size of the blocks to keeps
      blocksToKeep ++= profileWithBlocks.blocks //Adds all blocks to the queue, the bigger one will be automatically dropped
      ProfileBlocks(profileWithBlocks.profileID, blocksToKeep.toSet) //Return the new blocks*/
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
