package org.bd720.ercore.methods.similarityjoins.common.ed
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.similarityjoins.datastructure.Qgram
object CommonEdFunctions {
  object commons {
    def fixPrefix: (Int, Int) = (-1, -1)
  }
  def editDist[A](a: Iterable[A], b: Iterable[A]): Int = {
    ((0 to b.size).toList /: a) ((prev, x) =>
      (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
        case (h, ((d, v), y)) => math.min(math.min(h + 1, v + 1), d + (if (x == y) 0 else 1))
      }) last
  }
  def getQgrams(str: String, qgramSize: Int): Array[(String, Int)] = {
    str.sliding(qgramSize).zipWithIndex.map(q => (q._1, q._2)).toArray
  }
  def getQgramsTf(docs: RDD[(Int, Array[(String, Int)])]): Map[String, Int] = {
    val allQgrams = docs.flatMap { case (docId, qgrams) =>
      qgrams.map { case (str, pos) =>
        str
      }
    }
    allQgrams.groupBy(x => x).map(x => (x._1, x._2.size)).collectAsMap().toMap
  }
  def getSortedQgrams(docs: RDD[(Int, Array[(String, Int)])]): RDD[(Int, Array[(Int, Int)])] = {
    val tf = getQgramsTf(docs)
    val tf2 = docs.context.broadcast(tf.toList.sortBy(_._2).zipWithIndex.map(x => (x._1._1, x._2)).toMap)
    docs.map { case (docId, qgrams) =>
      val sortedQgrams = qgrams.map(q => (tf2.value(q._1), q._2)).sortBy(q => q)
      (docId, sortedQgrams)
    }
  }
  def getSortedQgrams2(docs: RDD[(Int, String, Array[(String, Int)])]): RDD[(Int, String, Array[(Int, Int)])] = {
    val tf = getQgramsTf(docs.map(x => (x._1, x._3)))
    val tf2 = docs.context.broadcast(
      tf.toList.sortBy(_._2).
        zipWithIndex.map(x => (x._1._1, x._2)).toMap)
    docs.map { case (docId, doc, qgrams) =>
      val sortedQgrams = qgrams.map(q => (tf2.value(q._1), q._2)).sortBy(q => q)
      (docId, doc, sortedQgrams)
    }
  }
  def buildPrefixIndex(sortedDocs: RDD[(Int, Array[(Int, Int)])], qgramLen: Int, threshold: Int): RDD[(Int, Array[Qgram])] = {
    val prefixLen = EdFilters.getPrefixLen(qgramLen, threshold)
    val allQgrams = sortedDocs.flatMap { case (docId, qgrams) =>
      val prefix = {
        if (qgrams.length < prefixLen) {
          qgrams.union(commons.fixPrefix :: Nil)
        }
        else {
          qgrams.take(prefixLen)
        }
      }
      prefix.zipWithIndex.map { case (qgram, index) =>
        (qgram._1, Qgram(docId, qgrams.length, qgram._2, index))
      }
    }
    allQgrams.groupBy(_._1).filter(_._2.size > 1).map(x => (x._1, x._2.map(_._2).toArray))
  }
}
