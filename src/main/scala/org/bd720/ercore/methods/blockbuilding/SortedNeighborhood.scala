package org.wumiguo.ser.methods.blockbuilding
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import org.wumiguo.ser.methods.blockbuilding.TokenBlocking.removeBadWords
import org.wumiguo.ser.methods.datastructure.{BlockAbstract, BlockClean, BlockDirty, KeyValue, Profile}
object SortedNeighborhood extends Serializable {
  val log = LoggerFactory.getLogger(getClass.getName)
  class KeyPartitioner(override val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = {
      key.asInstanceOf[Int]
    }
  }
  def createBlocks(profiles: RDD[Profile], slidingWindows: Int, separatorIDs: Array[Int] = Array.emptyIntArray, keysToExclude: Iterable[String] = Nil, removeStopWords: Boolean = false,
                   createKeysFunctions: (Iterable[KeyValue], Iterable[String]) => Iterable[String] = BlockingKeysStrategies.createKeysFromProfileAttributes): RDD[BlockAbstract] = {
    val tokensPerProfile = profiles.map(profile => (profile.id, createKeysFunctions(profile.attributes, keysToExclude).filter(_.trim.length > 0).toSet))
    val tokenVsProfileIDPair = if (removeStopWords) {
      removeBadWords(tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID))
    } else {
      tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID)
    }
    val sorted = tokenVsProfileIDPair.sortByKey()
    log.info("sorted numofpart {}",sorted.getNumPartitions)
    val partialRes = sorted.mapPartitionsWithIndex { case (partID, partData) =>
      val pDataLst = partData.toList
      log.info("partx " + partID + " pDataList " + pDataLst)
      val blocks = pDataLst.map(_._2).sliding(slidingWindows)
      log.info("blocks {}", blocks)
      val first = {
        if (partID != 0) {
          (partID - 1, pDataLst.take(slidingWindows - 1))
        }
        else {
          (partID, Nil)
        }
      }
      val last = {
        if (partID != sorted.getNumPartitions - 1) {
          (partID, pDataLst.takeRight(slidingWindows - 1))
        }
        else {
          (partID, Nil)
        }
      }
      List((blocks, first :: last :: Nil)).toIterator
    }
    val b1 = partialRes.flatMap(_._1)
    val b2 = partialRes.flatMap(_._2).partitionBy(new KeyPartitioner(sorted.getNumPartitions)).mapPartitions { part =>
      part.flatMap(_._2).toList.sortBy(_._1).map(_._2).sliding(slidingWindows)
    }
    val blocks = b1.union(b2)
    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean) */
    val profilesGrouped = blocks.map {
      c =>
        val entityIds = c.toSet
        val blockEntities = {
          if (separatorIDs.isEmpty) {
            Array(entityIds)
          }
          else {
            TokenBlocking.separateProfiles(entityIds, separatorIDs)
          }
        }
        blockEntities
    }
    /* Removes blocks that contains only one profile, and associate an unique ID to each block */
    val profilesGroupedWithIds = profilesGrouped filter {
      block =>
        if (separatorIDs.isEmpty) block.head.size > 1
        else block.count(_.nonEmpty) > 1
    } zipWithIndex()
    profilesGroupedWithIds map {
      case (entityIds, blockId) =>
        if (separatorIDs.isEmpty)
          BlockDirty(blockId.toInt, entityIds)
        else BlockClean(blockId.toInt, entityIds)
    }
  }
}
