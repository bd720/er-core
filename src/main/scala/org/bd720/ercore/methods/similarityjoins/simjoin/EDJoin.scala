package org.bd720.ercore.methods.similarityjoins.simjoin
import java.util.Calendar
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bd720.ercore.methods.datastructure.EDJoinPrefixIndexPartitioner
import org.bd720.ercore.methods.similarityjoins.common.ed.{CommonEdFunctions, EdFilters}
object EDJoin {
  def buildPrefixIndex(sortedDocs: RDD[(Int, String, Array[(Int, Int)])], qgramLen: Int, threshold: Int): RDD[(Int, Array[(Int, Int, Array[(Int, Int)], String)])] = {
    val prefixLen = EdFilters.getPrefixLen(qgramLen, threshold)
    val allQgrams = sortedDocs.flatMap { case (docId, doc, qgrams) =>
      val prefix = qgrams.take(prefixLen)
      prefix.zipWithIndex.map { case (qgram, index) =>
        (qgram._1, (docId, index, qgrams, doc))
      }
    }
    val blocks = allQgrams.groupByKey().filter(_._2.size > 1)
    blocks.map(b => (b._1, b._2.toArray.sortBy(_._3.length)))
  }
  def isLastCommonTokenPosition(doc1Tokens: Array[(Int, Int)], doc2Tokens: Array[(Int, Int)], currentToken: Int, qgramLen: Int, threshold: Int): Boolean = {
    val prefixLen = EdFilters.getPrefixLen(qgramLen, threshold)
    var d1Index = math.min(doc1Tokens.length - 1, prefixLen - 1)
    var d2Index = math.min(doc2Tokens.length - 1, prefixLen - 1)
    var valid = true
    var continue = true
    while (d1Index >= 0 && d2Index >= 0 && continue) {
      if (doc1Tokens(d1Index)._1 == doc2Tokens(d2Index)._1) {
        if (currentToken == doc1Tokens(d1Index)._1) {
          continue = false
        }
        else {
          continue = false
          valid = false
        }
      }
      else if (doc1Tokens(d1Index)._1 > doc2Tokens(d2Index)._1) {
        d1Index -= 1
      }
      else {
        d2Index -= 1
      }
    }
    valid
  }
  def getCandidatePairs(prefixIndex: RDD[(Int, Array[(Int, Int, Array[(Int, Int)], String)])], qgramLength: Int, threshold: Int): RDD[((Int, String), (Int, String))] = {
    val customPartitioner = new EDJoinPrefixIndexPartitioner(prefixIndex.getNumPartitions)
    val repartitionIndex = prefixIndex.map(_.swap).sortBy(x => -(x._1.length * (x._1.length - 1))).partitionBy(customPartitioner)
    repartitionIndex.flatMap { case (
      block /*string contain same token in the prefix*/ ,
      blockId /*tokenId*/ ) =>
      val results = new scala.collection.mutable.HashSet[((Int, String), (Int, String))]()
      var i = 0
      while (i < block.length) {
        var j = i + 1
        val d1Id = block(i)._1 // docId
        val d1Pos = block(i)._2 // index of the prefix
        val d1Qgrams = block(i)._3
        val d1 = block(i)._4 //the string 1
        while (j < block.length) {
          val d2Id = block(j)._1 // docId
          val d2Pos = block(j)._2 // index of the prefix
          val d2Qgrams = block(j)._3
          val d2 = block(j)._4 //the string 2
          if (d1Id != d2Id &&
            isLastCommonTokenPosition(d1Qgrams, d2Qgrams, blockId, qgramLength, threshold) &&
            math.abs(d1Pos - d2Pos) <= threshold &&
            math.abs(d1Qgrams.length - d2Qgrams.length) <= threshold
          ) {
            if (EdFilters.commonFilter(d1Qgrams, d2Qgrams, qgramLength, threshold)) {
              if (d1Id < d2Id) {
                results.add(((d1Id, d1), (d2Id, d2)))
              }
              else {
                results.add(((d2Id, d2), (d1Id, d1)))
              }
            }
          }
          j += 1
        }
        i += 1
      }
      results
    }
  }
  def getPositionalQGrams(documents: RDD[(Int, String)], qgramLength: Int): RDD[(Int, String, Array[(String, Int)])] = {
    documents.map(x => (x._1, x._2, CommonEdFunctions.getQgrams(x._2, qgramLength)))
  }
  def getCandidates(documents: RDD[(Int, String)], qgramLength: Int, threshold: Int): RDD[((Int, String), (Int, String))] = {
    val docs = getPositionalQGrams(documents, qgramLength)
    val log = LogManager.getRootLogger
    val sortedDocs = CommonEdFunctions.getSortedQgrams2(docs)
    sortedDocs.persist(StorageLevel.MEMORY_AND_DISK)
    log.info("[EDJoin] sortedcs count " + sortedDocs.count())
    val ts = Calendar.getInstance().getTimeInMillis
    val prefixIndex = buildPrefixIndex(sortedDocs, qgramLength, threshold)
    prefixIndex.persist(StorageLevel.MEMORY_AND_DISK)
    val np = prefixIndex.count()
    sortedDocs.unpersist()
    val te = Calendar.getInstance().getTimeInMillis
    log.info("[EDJoin] Number of elements in the index " + np)
    val a = prefixIndex.map(x => x._2.length.toDouble * (x._2.length - 1))
    val min = a.min()
    val max = a.max()
    val cnum = a.sum()
    val avg = cnum / np
    log.info("[EDJoin] Min number of comparisons " + min)
    log.info("[EDJoin] Max number of comparisons " + max)
    log.info("[EDJoin] Avg number of comparisons " + avg)
    log.info("[EDJoin] Estimated comparisons " + cnum)
    log.info("[EDJoin] EDJOIN index time (s) " + (te - ts) / 1000.0)
    val t1 = Calendar.getInstance().getTimeInMillis
    val candidates = getCandidatePairs(prefixIndex, qgramLength, threshold)
    val nc = candidates.count()
    prefixIndex.unpersist()
    val t2 = Calendar.getInstance().getTimeInMillis
    log.info("[EDJoin] Candidates number " + nc)
    log.info("[EDJoin] EDJOIN join time (s) " + (t2 - t1) / 1000.0)
    candidates
  }
  def getMatches(documents: RDD[(Int, String)], qgramLength: Int, threshold: Int): RDD[(Int, Int, Double)] = {
    val log = LogManager.getRootLogger
    log.info("[EDJoin] first document " + documents.first())
    val t1 = Calendar.getInstance().getTimeInMillis
    val candidates = getCandidates(documents, qgramLength, threshold)
    val t2 = Calendar.getInstance().getTimeInMillis
    val m = candidates.map { case ((d1Id, d1), (d2Id, d2)) => ((d1Id, d1), (d2Id, d2), CommonEdFunctions.editDist(d1, d2)) }
      .filter(_._3 <= threshold)
      .map { case ((d1Id, d1), (d2Id, d2), ed) => (d1Id, d2Id, ed.toDouble) }
    m.persist(StorageLevel.MEMORY_AND_DISK)
    val nm = m.count()
    val t3 = Calendar.getInstance().getTimeInMillis
    log.info("[EDJoin] Num matches " + nm)
    log.info("[EDJoin] Verify time (s) " + (t3 - t2) / 1000.0)
    log.info("[EDJoin] Global time (s) " + (t3 - t1) / 1000.0)
    m
  }
}
