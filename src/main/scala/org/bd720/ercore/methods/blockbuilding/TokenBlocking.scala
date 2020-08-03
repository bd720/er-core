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
    /* For each profile returns the list of his tokens, produces (profileID, [list of tokens]) */
    val tokensPerProfile = profiles.map(profile => (profile.id, createKeysFunctions(profile.attributes, keysToExclude).filter(_.trim.length > 0).toSet))
    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    val a = if (removeStopWords) {
      removeBadWords(tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID))
    } else {
      tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID)
    }
    val profilePerKey = a.groupByKey().filter(_._2.size > 1)
    /* For each token divides the profiles in two lists according to the datasets they come from (only for Clean-Clean) */
    val profilesGrouped = profilePerKey.map {
      c =>
        val entityIds = c._2.toSet
        val blockEntities = if (separatorIDs.isEmpty) Array(entityIds) else TokenBlocking.separateProfiles(entityIds, separatorIDs)
        blockEntities
    }
    /* Removes blocks that contains only one profile, and associate an unique ID to each block */
    val profilesGroupedWithIds = profilesGrouped filter {
      block =>
        if (separatorIDs.isEmpty) block.head.size > 1
        else block.count(_.nonEmpty) > 1
    } zipWithIndex()
    /* Map each row in an object BlockClean or BlockDirty */
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
    /* Generates the tokens for each profile */
    val tokensPerProfile1 = profiles.map {
      profile =>
        /* Calculates the dataset to which this token belongs */
        val dataset = profile.sourceId + clusterNameSeparator
        /* Generates the tokens for this profile */
        val tokens = profile.attributes.flatMap {
          keyValue =>
            if (keysToExclude.exists(_.equals(keyValue.key))) {
              Nil
            }
            else {
              val key = dataset + keyValue.key //Add the dataset suffix to the key
              val clusterID = clusterMap.getOrElse(key, defaultClusterID) //Gets the id of this key cluster, if it not exists returns the default one
              val values = keyValue.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).map(_.toLowerCase.trim).filter(_.trim.nonEmpty).distinct //Split the values and obtain the tokens
              values.map(_ + "_" + clusterID).map(x => (x, key)) //Add the cluster id to the tokens
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
    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    val profilePerKey = {
      if (excludeDefaultCluster) {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey().filter(!_._1.endsWith("_" + defaultClusterID))
      }
      else {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey()
      }
    }
    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean) */
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
    /* Removes blocks that contains only one profile, and associate an unique ID to each block */
    val profilesGroupedWithIds = profilesGrouped filter {
      case (block, entropy, clusterID, blockingKey) =>
        if (separatorIDs.isEmpty) block.head.size > 1
        else block.count(_.nonEmpty) > 1
    } zipWithIndex()
    /* Map each row in an object BlockClean or BlockDirty */
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
        /* Calculates the dataset to which this token belongs */
        val dataset = profile.sourceId + clusterNameSeparator
        /* Generates the tokens for this profile */
        val tokens = profile.attributes.flatMap {
          keyValue =>
            if (keysToExclude.exists(_.equals(keyValue.key))) {
              Nil
            }
            else {
              val key = dataset + keyValue.key //Add the dataset suffix to the key
              val clusterID = clusterMap.getOrElse(key, defaultClusterID) //Gets the id of this key cluster, if it not exists returns the default one
              val values = keyValue.value.spliockingUtils.TokenizerPattern.DEFAULT_SPLITTING).map(_.toLowerCase.trim).filter(_.trim.nonEmpty).distinct //Split the values and obtain the tokens
              values.map(_ + "_" + clusterID) //Add the cluster id to the tokens
            }
        }.filter(_.nonEmpty)
        (profile.id, tokens.distinct)
    }
    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    val profilePerKey = {
      if (excludeDefaultCluster) {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey().filter(!_._1.endsWith("_" + defaultClusterID))
      }
      else {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey()
      }
    }
    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean) */
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
    /* Removes blocks that contains only one profile, and associate an unique ID to each block */
    val profilesGroupedWithIds = profilesGrouped filter {
      case (block, entropy, clusterID, blockingKey) =>
        if (separatorIDs.isEmpty) block.head.size > 1
        else block.count(_.nonEmpty) > 1
    } zipWithIndex()
    /* Map each row in an object BlockClean or BlockDirty */
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
