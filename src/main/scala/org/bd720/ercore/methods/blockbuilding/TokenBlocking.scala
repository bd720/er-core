package org.bd720.ercore.methods.blockbuilding
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.blockbuilding.LSH.Settings
import org.bd720.ercore.methods.datastructure
import org.bd720.ercore.methods.datastructure.{BlockAbstract, BlockClean, BlockDirty, KeyValue, KeysCluster, Profile}
object TokenBlocking {
  def removeBadWords(input: RDD[(String, Int)]): RDD[(String, Int)] = {
    val sc = SparkContext.getOrCreate()
    val stopwords = sc.broadcast(StopWordsRemover.loadDefaultStopWords("english"))
    input
      .filter(x => x._1.matches("[A-Za-z]+") || x._1.matches("[0-9]+"))
      .filter(x => !stopwords.value.contains(x._1))
  }
  def createBlocks(profiles: RDD[Profile],
                   separatorIDs: Array[Int] = Array.emptyIntArray,
                   keysToExclude: Iterable[String] = Nil,
                   removeStopWords: Boolean = false,
                   createKeysFunctions: (Iterable[KeyValue], Iterable[String]) => Iterable[String] = BlockingKeysStrategies.createKeysFromProfileAttributes): RDD[BlockAbstract] = {
    val tokensPerProfile = profiles.map(profile => (profile.id, createKeysFunctions(profile.attributes, keysToExclude).filter(_.trim.length > 0).toSet))
    val a = if (removeStopWords) {
      removeBadWords(tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID))
    } else {
      tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID)
    }
    val profilePerKey = a.groupByKey().filter(_._2.size > 1)
    val profilesGrouped = profilePerKey.map {
      c =>
        val entityIds = c._2.toSet
        val blockEntities = if (separatorIDs.isEmpty) Array(entityIds) else TokenBlocking.separateProfiles(entityIds, separatorIDs)
        blockEntities
    }
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
  def createBlocksClusterDebug(profiles: RDD[Profile],
                               separatorIDs: Array[Int],
                               clusters: List[KeysCluster],
                               keysToExclude: Iterable[String] = Nil,
                               excludeDefaultCluster: Boolean = false,
                               clusterNameSeparator: String = Settings.SOURCE_NAME_SEPARATOR):
  (RDD[BlockAbstract], scala.collection.Map[String, Map[Int, Iterable[String]]]) = {
    val defaultClusterID = clusters.maxBy(_.id).id
    val entropies = clusters.map(cluster => (cluster.id, cluster.entropy)).toMap
    val clusterMap = clusters.flatMap(c => c.keys.map(key => (key, c.id))).toMap
    val tokensPerProfile1 = profiles.map {
      profile =>
        val dataset = profile.sourceId + clusterNameSeparator
        val tokens = profile.attributes.flatMap {
          keyValue =>
            if (keysToExclude.exists(_.equals(keyValue.key))) {
              Nil
            }
            else {
              val key = dataset + keyValue.key 
              val clusterID = clusterMap.getOrElse(key, defaultClusterID) 
              val values = keyValue.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).map(_.toLowerCase.trim).filter(_.trim.nonEmpty).distinct 
              values.map(_ + "_" + clusterID).map(x => (x, key)) 
            }
        }.filter(x => x._1.nonEmpty)
        (profile.id, tokens)
    }
    val debug = tokensPerProfile1.flatMap { case (profileId, tokens) =>
      tokens.map { case (token, attribute) =>
        (token, (profileId, attribute))
      }
    }.groupByKey().map(x => (x._1, x._2.groupBy(_._1).map(y => (y._1, y._2.map(_._2))))).collectAsMap()
    val tokensPerProfile = tokensPerProfile1.map(x => (x._1, x._2.map(_._1).distinct))
    val profilePerKey = {
      if (excludeDefaultCluster) {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey().filter(!_._1.endsWith("_" + defaultClusterID))
      }
      else {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey()
      }
    }
    val profilesGrouped = profilePerKey map {
      case (blockingKey, entityIds) =>
        val blockEntities = {
          if (separatorIDs.isEmpty) {
            Array(entityIds.toSet)
          }
          else {
            TokenBlocking.separateProfiles(entityIds.toSet, separatorIDs)
          }
        }
        var clusterID = defaultClusterID
        val entropy = {
          try {
            clusterID = blockingKey.split("_").last.toInt
            val e = entropies.get(clusterID)
            if (e.isDefined) e.get else 0.0
          }
          catch {
            case _: Throwable => 0.0
          }
        }
        (blockEntities, entropy, clusterID, blockingKey)
    }
    val profilesGroupedWithIds = profilesGrouped filter {
      case (block, entropy, clusterID, blockingKey) =>
        if (separatorIDs.isEmpty) block.head.size > 1
        else block.count(_.nonEmpty) > 1
    } zipWithIndex()
    val blocks: RDD[BlockAbstract] = profilesGroupedWithIds map {
      case ((entityIds, entropy, clusterID, blockingKey), blockId) =>
        if (separatorIDs.isEmpty) datastructure.BlockDirty(blockId.toInt, entityIds, entropy, clusterID, blockingKey = blockingKey)
        else datastructure.BlockClean(blockId.toInt, entityIds, entropy, clusterID, blockingKey = blockingKey)
    }
    (blocks, debug)
  }
  def createBlocksCluster(profiles: RDD[Profile],
                          separatorIDs: Array[Int],
                          clusters: List[KeysCluster],
                          keysToExclude: Iterable[String] = Nil,
                          excludeDefaultCluster: Boolean = false,
                          clusterNameSeparator: String = Settings.SOURCE_NAME_SEPARATOR): RDD[BlockAbstract] = {
    val defaultClusterID = clusters.maxBy(_.id).id
    val entropies = clusters.map(cluster => (cluster.id, cluster.entropy)).toMap
    val clusterMap = clusters.flatMap(c => c.keys.map(key => (key, c.id))).toMap
    val tokensPerProfile = profiles.map {
      profile =>
        val dataset = profile.sourceId + clusterNameSeparator
        val tokens = profile.attributes.flatMap {
          keyValue =>
            if (keysToExclude.exists(_.equals(keyValue.key))) {
              Nil
            }
            else {
              val key = dataset + keyValue.key 
              val clusterID = clusterMap.getOrElse(key, defaultClusterID) 
              val values = keyValue.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).map(_.toLowerCase.trim).filter(_.trim.nonEmpty).distinct 
              values.map(_ + "_" + clusterID) 
            }
        }.filter(_.nonEmpty)
        (profile.id, tokens.distinct)
    }
    val profilePerKey = {
      if (excludeDefaultCluster) {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey().filter(!_._1.endsWith("_" + defaultClusterID))
      }
      else {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey()
      }
    }
    val profilesGrouped = profilePerKey map {
      case (blockingKey, entityIds) =>
        val blockEntities = {
          if (separatorIDs.isEmpty) {
            Array(entityIds.toSet)
          }
          else {
            TokenBlocking.separateProfiles(entityIds.toSet, separatorIDs)
          }
        }
        var clusterID = defaultClusterID
        val entropy = {
          try {
            clusterID = blockingKey.split("_").last.toInt
            val e = entropies.get(clusterID)
            if (e.isDefined) {
              e.get
            }
            else {
              0.0
            }
          }
          catch {
            case _: Throwable => 0.0
          }
        }
        (blockEntities, entropy, clusterID, blockingKey)
    }
    val profilesGroupedWithIds = profilesGrouped filter {
      case (block, entropy, clusterID, blockingKey) =>
        if (separatorIDs.isEmpty) block.head.size > 1
        else block.count(_.nonEmpty) > 1
    } zipWithIndex()
    profilesGroupedWithIds map {
      case ((entityIds, entropy, clusterID, blockingKey), blockId) =>
        if (separatorIDs.isEmpty) datastructure.BlockDirty(blockId.toInt, entityIds, entropy, clusterID, blockingKey = blockingKey)
        else datastructure.BlockClean(blockId.toInt, entityIds, entropy, clusterID, blockingKey = blockingKey)
    }
  }
  def separateProfiles(elements: Set[Int], separators: Array[Int]): Array[Set[Int]] = {
    var input = elements
    var output: List[Set[Int]] = Nil
    separators.foreach { sep =>
      val a = input.partition(_ <= sep)
      input = a._2
      output = a._1 :: output
    }
    output = input :: output
    output.reverse.toArray
  }
}
