package org.bd720.ercore.methods.similarityjoins.simjoin
import java.util.Calendar
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.datastructure.PPJoinPrefixIndexPartitioner
import org.bd720.ercore.methods.similarityjoins.common.js.{CommonJsFunctions, JsFilters}
object PPJoin {
  def lastCommonTokenPosition(doc1Tokens: Array[Int], doc2Tokens: Array[Int], currentToken: Int, prefixLen1: Int, prefixLen2: Int): (Int, Int, Boolean) = {
    var d1Index = prefixLen1 - 1
    var d2Index = prefixLen2 - 1
    var valid = true
    var continue = true
    while (d1Index >= 0 && d2Index >= 0 && continue) {
      if (doc1Tokens(d1Index) == doc2Tokens(d2Index)) {
        if (currentToken == doc1Tokens(d1Index)) {
          continue = false
        }
        else {
          continue = false
          valid = false
        }
      }
      else if (doc1Tokens(d1Index) > doc2Tokens(d2Index)) {
        d1Index -= 1
      }
      else {
        d2Index -= 1
      }
    }
    (d1Index, d2Index, valid)
  }
  def getCommonElementsInPrefix(doc1Tokens: Array[Int], doc2Tokens: Array[Int], sPos1: Int, sPos2: Int): Int = {
    var common = 1
    var p1 = sPos1 - 1
    var p2 = sPos2 - 1
    while (p1 >= 0 && p2 >= 0) {
      if (doc1Tokens(p1) == doc2Tokens(p2)) {
        common = common + 1
        p1 -= 1
        p2 -= 1
      }
      else if (doc1Tokens(p1) > doc2Tokens(p2)) {
        p1 -= 1
      }
      else {
        p2 -= 1
      }
    }
    common
  }
  def getCandidatePairs(prefixIndex: RDD[(Int, Array[(Int, Array[Int])])], threshold: Double, separatorID: Int): RDD[((Int, Array[Int]), (Int, Array[Int]))] = {
    val customPartitioner = new PPJoinPrefixIndexPartitioner(prefixIndex.getNumPartitions)
    val repartitionIndex = prefixIndex.map(_.swap).partitionBy(customPartitioner)
    repartitionIndex.flatMap {
      case (docs, tokenId) =>
        val results = scala.collection.mutable.Set[((Int, Array[Int]), (Int, Array[Int]))]()
        var i = 0
        while (i < docs.length - 1) {
          var j = i + 1
          val doc1Id = docs(i)._1
          val doc1Tokens = docs(i)._2
          val doc1PrefixLen = JsFilters.getPrefixLength(doc1Tokens.length, threshold)
          while ((j < docs.length) && (doc1Tokens.length >= docs(j)._2.length * threshold)) { 
            val doc2Id = docs(j)._1
            if (separatorID < 0 || ((doc1Id <= separatorID && doc2Id > separatorID) || (doc2Id <= separatorID && doc1Id > separatorID))) {
              val doc2Tokens = docs(j)._2
              val doc2PrefixLen = JsFilters.getPrefixLength(doc2Tokens.length, threshold)
              val (p1, p2, isLastCommon) = lastCommonTokenPosition(doc1Tokens, doc2Tokens, tokenId, doc1PrefixLen, doc2PrefixLen)
              if (isLastCommon) {
                val common = getCommonElementsInPrefix(doc1Tokens, doc2Tokens, p1, p2) 
                if (JsFilters.positionFilter(doc1Tokens.length, doc2Tokens.length, p1 + 1, p2 + 1, common, threshold)) {
                  if (doc1Id < doc2Id) {
                    results.add(((doc1Id, doc1Tokens), (doc2Id, doc2Tokens)))
                  }
                  else {
                    results.add(((doc2Id, doc2Tokens), (doc1Id, doc1Tokens)))
                  }
                }
              }
            }
            j = j + 1
          }
          i += 1
        }
        results
    }
  }
  def buildPrefixIndex(tokenizedDocOrd: RDD[(Int, Array[Int])], threshold: Double): RDD[(Int, Array[(Int, Array[Int])])] = {
    val indices = tokenizedDocOrd.flatMap {
      case (docId, tokens) =>
        val prefix = JsFilters.getPrefix(tokens, threshold)
        prefix.zipWithIndex.map {
          case (token, pos) =>
            (token, (docId, tokens))
        }
    }
    indices.groupByKey().filter(_._2.size > 1).map {
      case (tokenId, documents) => (tokenId, documents.toArray.sortBy(x => x._2.length))
    }
  }
  def getCandidates(tokenizedDocSort: RDD[(Int, Array[Int])], threshold: Double, separatorID: Int = -1): RDD[((Int, Array[Int]), (Int, Array[Int]))] = {
    val ts = Calendar.getInstance().getTimeInMillis
    val prefixIndex = buildPrefixIndex(tokenizedDocSort, threshold)
    prefixIndex.count()
    val log = LogManager.getRootLogger
    val te = Calendar.getInstance().getTimeInMillis
    log.info("[PPJOIN] PPJOIN index time (s) " + (te - ts) / 1000.0)
    val t1 = Calendar.getInstance().getTimeInMillis
    val candidates = getCandidatePairs(prefixIndex, threshold, separatorID)
    val cn = candidates.count()
    val t2 = Calendar.getInstance().getTimeInMillis
    log.info("[PPJOIN] PPJOIN join time (s) " + (t2 - t1) / 1000.0)
    log.info("[PPJOIN] Number of candidates " + cn)
    candidates
  }
  def calcJS(d1: Array[Int], d2: Array[Int]): Double = {
    val commons = d1.intersect(d2).length.toDouble
    commons / (d1.length + d2.length - commons)
  }
  def getMatches(documents: RDD[(Int, String)], threshold: Double, separatorID: Int = -1): RDD[(Int, Int)] = {
    val log = LogManager.getRootLogger
    val tokenizedDocSort = CommonJsFunctions.tokenizeAndSort(documents)
    val candidates = getCandidates(tokenizedDocSort, threshold, separatorID)
    val t1 = Calendar.getInstance().getTimeInMillis
    val matches = candidates
      .filter { case ((d1Id, d1Tokens), (d2Id, d2Tokens)) => calcJS(d1Tokens, d2Tokens) >= threshold }
      .map { case ((d1Id, d1Tokens), (d2Id, d2Tokens)) => (d1Id, d2Id) }
    matches.cache()
    val nm = matches.count()
    val t2 = Calendar.getInstance().getTimeInMillis
    log.info("[PPJOIN] PPJOIN verify time (s) " + (t2 - t1) / 1000.0)
    log.info("[PPJOIN] Number of matches " + nm)
    matches
  }
}
