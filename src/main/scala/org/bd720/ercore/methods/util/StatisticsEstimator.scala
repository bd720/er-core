package org.bd720.ercore.methods.util
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.datastructure
import org.bd720.ercore.methods.datastructure._
object StatisticsEstimator {
  def estimatePCandPQ(groundtruth: RDD[MatchingEntities], profiles: RDD[Profile], edges: RDD[WeightedEdge], realIDKey: String, firstDatasetMaxID: Long = -1): (Double, Double) = {
    val profilesWithRealID = profiles.map(p => (p.id, p.getAttributeValues(realIDKey)))
    val candidatesCouplesID = edges.map(
      e =>
        if (firstDatasetMaxID >= 0 && e.firstProfileID > firstDatasetMaxID) {
          (e.secondProfileID, e.firstProfileID)
        }
        else {
          (e.firstProfileID, e.secondProfileID)
        }
    )
    val candidatesCouples = candidatesCouplesID.join(profilesWithRealID).map(_._2).join(profilesWithRealID).map(x => MatchingEntities(x._2._1, x._2._2))
    val common = groundtruth.intersection(candidatesCouples)
    val foundMatch = common.count()
    val numCandidates = candidatesCouples.count()
    val totalPerfectMatch = groundtruth.count()
    ((foundMatch.toDouble / totalPerfectMatch), (foundMatch.toDouble / numCandidates))
  }
  def estimatePCandPQ2(groundtruth: RDD[MatchingEntities], profiles: RDD[Profile], edges: RDD[UnweightedEdge], realIDKey: String, firstDatasetMaxID: Long = -1): (Double, Double) = {
    val profilesWithRealID = profiles.map(p => (p.id, p.getAttributeValues(realIDKey)))
    val candidatesCouplesID = edges.map(
      e =>
        if (firstDatasetMaxID >= 0 && e.firstProfileID > firstDatasetMaxID) {
          (e.secondProfileID, e.firstProfileID)
        }
        else {
          (e.firstProfileID, e.secondProfileID)
        }
    )
    val candidatesCouples = candidatesCouplesID.join(profilesWithRealID).map(_._2).join(profilesWithRealID).map(x => datastructure.MatchingEntities(x._2._1, x._2._2))
    val common = groundtruth.intersection(candidatesCouples)
    val foundMatch = common.count()
    val numCandidates = candidatesCouples.count()
    val totalPerfectMatch = groundtruth.count()
    ((foundMatch.toDouble / totalPerfectMatch), (foundMatch.toDouble / numCandidates))
  }
  def estimatePCPQ_uniqueRealID(groundTruth: RDD[MatchingEntities], profiles: RDD[Profile], candidates: RDD[(Int, Int)], realIDKey: String): (Double, Double) = {
    val sc = SparkContext.getOrCreate()
    println(profiles.map(_.getAttributeValues(realIDKey)).distinct.count)
    println(profiles.map(_.id).distinct.count)
    val profilesWithRealID = sc.broadcast(profiles.map(p => (p.getAttributeValues(realIDKey), p.id)).collectAsMap)
    def combinerAdd(xs: Set[Int], x: Int): Set[Int] = xs + x
    def combinerMerge(xs: Set[Int], ys: Set[Int]): Set[Int] = xs ++ ys
    val gt =
      sc.broadcast(
        groundTruth
          .map(me => {
            val pid = (profilesWithRealID.value(me.firstEntityID), profilesWithRealID.value(me.secondEntityID))
            pid match {
              case (pid1, pid2) if pid1 < pid2 => (pid1, pid2)
              case _ => (pid._2, pid._1)
            }
          })
          .combineByKey(x => Set(x), combinerAdd, combinerMerge)
          .collectAsMap
      )
    profilesWithRealID.destroy
    val foundMatch = candidates.filter(e => {
      gt.value.getOrElse(e._1, Set[Int]()) contains e._2
    }).distinct.count
    gt.destroy
    (foundMatch.toDouble / groundTruth.count, foundMatch.toDouble / candidates.count)
  }
  def estimatePCPQ(groundTruth: RDD[MatchingEntities], profiles: RDD[Profile], candidates: RDD[(Int, Int)], realIDKey: String = ""): (Double, Double) = {
    val sc = SparkContext.getOrCreate()
    val profilesWithRealID = {
      if (realIDKey.isEmpty) {
        sc.broadcast(profiles.map(p => (p.id, p.originalID)).collectAsMap)
      }
      else {
        sc.broadcast(profiles.map(p => (p.id, p.getAttributeValues(realIDKey))).collectAsMap)
      }
    }
    def combinerAdd(xs: Set[String], x: String): Set[String] = xs + x
    def combinerMerge(xs: Set[String], ys: Set[String]): Set[String] = xs ++ ys
    val gt =
      sc.broadcast(
        groundTruth.map(x => (x.firstEntityID, x.secondEntityID))
          .combineByKey(x => Set(x), combinerAdd, combinerMerge)
          .collectAsMap
      )
    val foundMatch = candidates.filter(e => {
      gt.value.getOrElse(profilesWithRealID.value(e._1), Set[String]()) contains profilesWithRealID.value(e._2)
    }).distinct.count
    gt.destroy
    profilesWithRealID.destroy
    (foundMatch.toDouble / groundTruth.count, foundMatch.toDouble / candidates.count)
  }
  def test(groundTruth: RDD[MatchingEntities], profiles: RDD[Profile], candidates: RDD[(Int, Int)], realIDKey: String) = {
    val sc = SparkContext.getOrCreate()
    val profilesWithRealID = sc.broadcast(profiles.map(p => (p.getAttributeValues(realIDKey), p.id)).collectAsMap)
    def combinerAdd(xs: Set[Int], x: Int): Set[Int] = xs + x
    def combinerMerge(xs: Set[Int], ys: Set[Int]): Set[Int] = xs ++ ys
    val a = groundTruth
      .map(me => {
        val pid = (profilesWithRealID.value(me.firstEntityID), profilesWithRealID.value(me.secondEntityID))
        pid match {
          case (pid1, pid2) if pid1 < pid2 => (pid1, pid2)
          case _ => (pid._2, pid._1)
        }
      })
      .combineByKey(x => Set(x), combinerAdd, combinerMerge)
    val gt =
      sc.broadcast(a.collectAsMap)
    val foundMatch = candidates.filter(e => {
      gt.value.getOrElse(e._1, Set[Int]()) contains e._2
    })
    val profilesWithRealID1 = sc.broadcast(profiles.map(p => (p.id, p.getAttributeValues(realIDKey))).collect.toMap)
    val candidatesCouples = candidates.map(c => UnweightedEdge(c._1, c._2).getEntityMatch(profilesWithRealID1.value))
    val mapGroundTruth = SparkContext.getOrCreate().broadcast(groundTruth.map(gt => (gt.firstEntityID, gt.secondEntityID)).collect.toMap)
    val common =
      candidatesCouples
        .map(c => (c.firstEntityID, c.secondEntityID))
        .groupByKey().filter(containsKey(mapGroundTruth.value))
        .map(x => (profilesWithRealID.value(x._1), profilesWithRealID.value(mapGroundTruth.value(x._1))))
    val test = common.subtract(foundMatch).distinct()
    println(test.count)
    println(candidates.count)
    println(test.intersection(candidates).count)
    println(test.intersection(a.flatMap(x => for (y <- x._2) yield (x._1, y))).count)
  }
  def estimatePCandPQbroadcast(groundtruth: RDD[MatchingEntities], profiles: RDD[Profile], edges: RDD[WeightedEdge], realIDKey: String, isCleanClean: Boolean = false): (Double, Double) = {
    val profilesWithRealID = SparkContext.getOrCreate().broadcast(profiles.map(p => (p.id, p.getAttributeValues(realIDKey))).collect.toMap)
    val candidatesCouples = edges.map(edge => edge.getEntityMatch(profilesWithRealID.value))
    val foundMatch = comparisonCandidatesGroundtruth(groundtruth, candidatesCouples, isCleanClean)
    profilesWithRealID.destroy()
    val numCandidates = edges.count()
    val totalPerfectMatch = groundtruth.count()
    (foundMatch.toDouble / totalPerfectMatch, foundMatch.toDouble / numCandidates)
  }
  def estimatePCandPQbroadcast2(groundtruth: RDD[MatchingEntities], profiles: RDD[Profile], edges: RDD[UnweightedEdge], realIDKey: String, isCleanClean: Boolean = false): (Double, Double) = {
    val profilesWithRealID = SparkContext.getOrCreate().broadcast(profiles.map(p => (p.id, p.getAttributeValues(realIDKey))).collect.toMap)
    val candidatesCouples = edges.map(edge => edge.getEntityMatch(profilesWithRealID.value))
    val foundMatch = comparisonCandidatesGroundtruth(groundtruth, candidatesCouples, isCleanClean)
    profilesWithRealID.destroy()
    val numCandidates = edges.count()
    val totalPerfectMatch = groundtruth.count()
    println("Matches2: " + foundMatch)
    (foundMatch.toDouble / totalPerfectMatch, foundMatch.toDouble / numCandidates)
  }
  def comparisonCandidatesGroundtruth(groundtruth: RDD[MatchingEntities], candidates: RDD[MatchingEntities], isCleanClean: Boolean): Long = {
    if (isCleanClean) {
      val mapGroundTruth = SparkContext.getOrCreate().broadcast(groundtruth.map(gt => (gt.firstEntityID, gt.secondEntityID)).collect.toMap)
      val commonCounter =
        candidates.map(c => (c.firstEntityID, c.secondEntityID)).groupByKey().filter(containsKey(mapGroundTruth.value)).count()
      mapGroundTruth.destroy()
      commonCounter
    }
    else
      groundtruth.intersection(candidates).count()
  }
  def containsKey(map: Map[String, String])(input: (String, Iterable[String])): Boolean = {
    if (!map.contains(input._1))
      false
    else {
      val matchValue = map(input._1)
      def loop(list: List[String]): Boolean = {
        if (list == Nil) false
        else if (list.head == matchValue) true
        else loop(list.tail)
      }
      loop(input._2.toList)
    }
  }
  def estimateNormalizedEntropy(blocks: RDD[BlockAbstract], profileBlocks: Broadcast[Map[Long, ProfileBlocks]]): RDD[(Long, Double)] = {
    val keysPerBlock = blocks.map {
      block =>
        val allKeys = block.getAllProfiles.flatMap {
          profileID =>
            profileBlocks.value(profileID).blocks.map(_.blockID)
        }
        (block.blockID, allKeys, block.size)
    }
    keysPerBlock map {
      blockWithKeys =>
        val blockID = blockWithKeys._1
        val keys = blockWithKeys._2
        val blockSize = blockWithKeys._3.toDouble
        val numberOfKeys = keys.length.toDouble
        val entropy = -keys.groupBy(x => x).map(x => (x._2.length)).map(s => (s / numberOfKeys) * Math.log(s.toDouble / numberOfKeys)).sum / numberOfKeys
        (blockID, entropy / blockSize)
    }
  }
  def estimateEntropy(blocks: RDD[BlockAbstract], profileBlocks: Broadcast[Map[Long, ProfileBlocks]]): RDD[(Long, Double)] = {
    val keysPerBlock = blocks.map {
      block =>
        val allKeys = block.getAllProfiles.flatMap {
          profileID =>
            profileBlocks.value(profileID).blocks.map(_.blockID)
        }
        (block.blockID, allKeys)
    }
    keysPerBlock map {
      blockWithKeys =>
        val blockID = blockWithKeys._1
        val keys = blockWithKeys._2
        val numberOfKeys = keys.length.toDouble
        val entropy = -keys.groupBy(x => x).map(x => (x._2.length)).map(s => (s / numberOfKeys) * Math.log(s.toDouble / numberOfKeys)).sum / numberOfKeys
        (blockID, entropy)
    }
  }
}
