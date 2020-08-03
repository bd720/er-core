package org.bd720.ercore.methods.entitymatching
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bd720.ercore.methods.datastructure.{Profile, UnweightedEdge, WeightedEdge}
import scala.collection.mutable
object EntityMatching {
  def getComparisons(candidatePairs: RDD[UnweightedEdge], profiles: RDD[(Int, Profile)]): RDD[(Profile, Array[Int])] = {
    candidatePairs
      .map(p => (p.firstProfileID.toInt, Array(p.secondProfileID.toInt)))
      .reduceByKey(_ ++ _)
      .leftOuterJoin(profiles)
      .map(p => (p._2._2.get, p._2._1))
  }
  def getProfilesMap(profiles: RDD[(Int, Profile)]): RDD[Map[Int, Profile]] = {
    profiles
      .mapPartitions {
        partitions =>
          val m = partitions.map(p => Map(p._1 -> p._2)).reduce(_ ++ _)
          Iterator(m)
      }
  }
  def profileMatching(profile1: Profile, profile2: Profile, matchingFunction: (Profile, Profile) => Double)
  : WeightedEdge = {
    val similarity = matchingFunction(profile1, profile2)
    WeightedEdge(profile1.id, profile2.id, similarity)
  }
  def groupLinkage(profile1: Profile, profile2: Profile, threshold: Double = 0.5)
  : WeightedEdge = {
    val similarityQueue: mutable.PriorityQueue[(Double, (String, String))] = MatchingFunctions.getSimilarityEdges(profile1, profile2, threshold)
    if (similarityQueue.nonEmpty) {
      var edgesSet: Set[String] = Set()
      var nominator = 0.0
      var denominator = (profile1.attributes.length + profile2.attributes.length).toDouble
      similarityQueue.dequeueAll.foreach {
        edge =>
          if (!edgesSet.contains(edge._2._1) && !edgesSet.contains(edge._2._2)) {
            edgesSet += (edge._2._1, edge._2._2)
            nominator += edge._1
            denominator -= 1.0
          }
      }
      WeightedEdge(profile1.id, profile2.id, nominator / denominator)
    }
    else
      WeightedEdge(profile1.id, profile2.id, -1)
  }
  def entityMatchingCB(profiles: RDD[Profile], candidatePairs: RDD[UnweightedEdge], bcstep: Int,
                       matchingMethodLabel: String = "pm", threshold: Double = 0.5,
                       matchingFunctions: (Profile, Profile) => Double = MatchingFunctions.jaccardSimilarity)
  : (RDD[WeightedEdge], Long) = {
    val matcher = if (matchingMethodLabel == "pm")
      (p1: Profile, p2: Profile) => {
        this.profileMatching(p1, p2, matchingFunctions)
      }
    else (p1: Profile, p2: Profile) => {
      this.groupLinkage(p1, p2, threshold)
    }
    val sc = SparkContext.getOrCreate()
    val profilesID = profiles.map(p => (p.id.toInt, p))
    val comparisonsRDD = getComparisons(candidatePairs, profilesID)
      .setName("ComparisonsPerPartition")
      .persist(StorageLevel.MEMORY_AND_DISK)
    comparisonsRDD.foreach(x=>println("comparisonsRDD="+x))
    val profilesMap = getProfilesMap(profilesID)
      .setName("profilesMap")
      .cache()
    profilesMap.foreach(x=>println("profilesMap="+x))
    var edgesArrayRDD: Array[RDD[WeightedEdge]] = Array()
    val step = if (bcstep > 0) bcstep else comparisonsRDD.getNumPartitions
    val partitionGroupIter = (0 until comparisonsRDD.getNumPartitions).grouped(step)
    while (partitionGroupIter.hasNext) {
      val partitionGroup = partitionGroupIter.next()
      println("pg:"+partitionGroup)
      val comparisonsPart = comparisonsRDD
        .mapPartitionsWithIndex((index, it) => if (partitionGroup.contains(index)) it else Iterator(), preservesPartitioning = true)
        .collect()
      comparisonsPart.foreach(x=>println("comparisonsPart:"+x))
      val comparisonsPartBD = sc.broadcast(comparisonsPart)
      val wEdges = profilesMap
        .flatMap { profilesMap1 =>
          comparisonsPartBD.value
            .flatMap { c =>
              println("loopc:"+c._1 + ", " + c._2)
              val profile1 = c._1
              val comparisons = c._2
              comparisons
                .filter(profilesMap1.contains)
                .map(profilesMap1(_))
                .map(matcher(profile1, _))
                .filter(_.weight >=.5)
            }
        }
      wEdges.foreach(x=>println("+ wEdges="+x))
      edgesArrayRDD = edgesArrayRDD :+ wEdges
    }
    val matches = sc.union(edgesArrayRDD)
      .setName("Matches")
      .persist(StorageLevel.MEMORY_AND_DISK)
    val matchesCount = matches.count()
    matches.foreach(x=>println("matches="+x))
    println("count+"+matchesCount)
    profilesMap.unpersist()
    comparisonsRDD.unpersist()
    (matches, matchesCount)
  }
  def entityMatchingJOIN(profiles: RDD[Profile], candidatePairs: RDD[UnweightedEdge], bcstep: Int,
                         matchingMethodLabel: String = "pm", threshold: Double = 0.5,
                         matchingFunctions: (Profile, Profile) => Double = MatchingFunctions.jaccardSimilarity)
  : (RDD[WeightedEdge], Long) = {
    val matcher = if (matchingMethodLabel == "pm")
      (p1: Profile, p2: Profile) => {
        this.profileMatching(p1, p2, matchingFunctions)
      }
    else (p1: Profile, p2: Profile) => {
      this.groupLinkage(p1, p2, threshold)
    }
    val profilesID = profiles.map(p => (p.id.toInt, p))
    val comparisonsRDD = getComparisons(candidatePairs, profilesID)
      .flatMap(x => x._2.map(id => (id, Array(x._1))))
      .reduceByKey(_ ++ _)
      .leftOuterJoin(profilesID)
      .map(p => (p._2._2.orNull, p._2._1))
      .filter(_._1 != null)
      .setName("ComparisonsRDD")
      .persist(StorageLevel.MEMORY_AND_DISK)
    val matches = comparisonsRDD
      .flatMap { pc =>
        val profile1 = pc._1
        val comparisons = pc._2
        comparisons
          .map(matcher(profile1, _))
          .filter(_.weight >= 0.5)
      }
      .persist(StorageLevel.MEMORY_AND_DISK)
    (matches, matches.count)
  }
}
