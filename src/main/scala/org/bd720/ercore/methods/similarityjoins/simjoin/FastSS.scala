package org.bd720.ercore.methods.similarityjoins.simjoin
import java.util.Calendar
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
object FastSS {
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
  def buildIndex(profiles: RDD[(Int, String)], threshold: Int): RDD[(String, Map[Int, Iterable[String]])] = {
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
    index.filter(_._2.size > 1)
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
  def getIntMatches(index: RDD[(String, Map[Int, Iterable[String]])], threshold: Int, maxProfileId: Int): RDD[(Int, Int)] = {
    val t2 = Calendar.getInstance().getTimeInMillis
    val sc = SparkContext.getOrCreate()
    val blocks = index.zipWithIndex().map { case ((str, index), id) => (id.toInt, index, str) }
    val profilesToBlocks = blocks.flatMap { case (blockId, profiles, str) => profiles.keySet.map(pId => (pId, blockId)) }.groupByKey()
    val blocksBrd = sc.broadcast(blocks.map(x => (x._1, x._2)).collectAsMap())
    val log = LogManager.getRootLogger
    val size = SizeEstimator.estimate(blocksBrd.value)
    log.info("[FastSS] Estimated block size (bytes) " + size)
    val matches = profilesToBlocks.mapPartitions { part =>
      var numNeighbors = 0
      val neighbors = Array.ofDim[Int](maxProfileId)
      val added = Array.fill[Boolean](maxProfileId)(false)
      val results = new scala.collection.mutable.HashSet[(Int, Int)]()
      part.foreach { case (docId, blocks) =>
        blocks.foreach { block =>
          val tmp = blocksBrd.value.get(block)
          if (tmp.isDefined) {
            val elements = tmp.get
            val tmp2 = elements.get(docId)
            if (tmp2.isDefined) {
              val docPositions = tmp2.get
              elements.foreach { case (nId, nPositions) =>
                if (docId < nId && !added(nId)) {
                  docPositions.foreach { d1Pos =>
                    nPositions.foreach { d2Pos =>
                      if (!added(nId) && checkEditDistance(d1Pos, d2Pos) <= threshold) {
                        added.update(nId, true)
                        neighbors.update(numNeighbors, nId)
                        numNeighbors += 1
                      }
                    }
                  }
                }
              }
            }
          }
        }
        for (i <- 0 until numNeighbors) {
          results.add((docId, neighbors(i)))
          added.update(neighbors(i), false)
        }
        numNeighbors = 0
      }
      results.toIterator
    }
    matches.persist(StorageLevel.MEMORY_AND_DISK)
    val nc = matches.count()
    log.info("[FastSS] Num candidates  " + nc)
    val t3 = Calendar.getInstance().getTimeInMillis
    log.info("[FastSS] Candidates time (s) " + (t3 - t2) / 1000)
    blocksBrd.unpersist()
    matches
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
