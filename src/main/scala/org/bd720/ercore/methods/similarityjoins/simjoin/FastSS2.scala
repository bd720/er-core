package org.bd720.ercore.methods.similarityjoins.simjoin
import java.util.Calendar
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
object FastSS2 {
  def performDeletions(recordId: Int, str: String, sk: Int): Iterable[(String, (Int, String))] = {
    var delPositions = ""
    val results = new scala.collection.mutable.ArrayBuffer[(String, (Int, String))]()
    def Ud(str: String, k: Int): Unit = {
      if (k == 0) {
        results.append((str, (recordId, delPositions)))
      }
      else {
        val startPos = {
          if (delPositions.isEmpty) {
            0
          }
          else {
            delPositions.last - '0'
          }
        }
        for (pos <- startPos until str.length) {
          delPositions += pos
          Ud(str.substring(0, pos) + str.substring(pos + 1), k - 1)
          delPositions = delPositions.dropRight(1)
        }
      }
    }
    Ud(str, sk)
    results
  }
  def buildIndex(profiles: RDD[(Int, String)], threshold: Int): RDD[(String, Array[(Int, Iterable[String])])] = {
    val delPos = profiles.map { case (profileID, value) =>
      val delPositions = for (k <- 0 to threshold) yield {
        performDeletions(profileID, value, k)
      }
      val delPosAll = delPositions.reduce((d1, d2) => d1 ++ d2)
      delPosAll
    }
    val index = delPos.flatMap(x => x)
      .groupByKey()
      .map { g =>
        (g._1, g._2.groupBy(_._1).map(x => (x._1, x._2.map(_._2))))
      }
      .map(x => (x._1, x._2.toArray))
    index.filter(_._2.length > 1)
  }
  def checkEditDistance(p1: String, p2: String): Int = {
    var i = 0
    var j = 0
    var updates = 0
    while (i < p1.length && j < p2.length) {
      if (p1(i) == p2(j)) {
        updates += 1
        i += 1
        j += 1
      }
      else if (p1(i) < p2(j)) {
        i += 1
      }
      else {
        j += 1
      }
    }
    p1.length + p2.length - updates
  }
  def getIntMatches(index: RDD[(String, Array[(Int, Iterable[String])])], threshold: Int, maxProfileId: Int): RDD[(Int, Int)] = {
    index.flatMap { case (key, indexEntry) =>
      val matches = for (i <- indexEntry.indices; j <- indexEntry.indices; if i < j) yield {
        for (d1Pos <- indexEntry(i)._2; d2Pos <- indexEntry(j)._2; if checkEditDistance(d1Pos, d2Pos) <= threshold) yield {
          if (indexEntry(i)._1 < indexEntry(j)._1) {
            (indexEntry(i)._1, indexEntry(j)._1)
          }
          else {
            (indexEntry(j)._1, indexEntry(i)._1)
          }
        }
      }
      matches.flatten.toSet
    }.distinct()
  }
  def getMatches(profiles: RDD[(Int, String)], threshold: Int): RDD[(Int, Int)] = {
    val t1 = Calendar.getInstance().getTimeInMillis
    val index = buildIndex(profiles, threshold)
    index.persist(StorageLevel.MEMORY_AND_DISK)
    val ni = index.count()
    val t2 = Calendar.getInstance().getTimeInMillis
    val log = LogManager.getRootLogger
    log.info("[FastSS] Num index " + ni)
    log.info("[FastSS] Index time (s) " + (t2 - t1) / 1000)
    val results = getIntMatches(index, threshold, profiles.map(_._1).max() + 1)
    index.unpersist()
    val t3 = Calendar.getInstance().getTimeInMillis
    log.info("[FastSS] Tot time (s) " + (t3 - t1) / 1000)
    results
  }
}
