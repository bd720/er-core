package org.bd720.ercore.methods.similarityjoins.simjoin
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.datastructure.KeyValue
import org.bd720.ercore.methods.entitymatching.MatchingFunctions
import org.bd720.ercore.methods.similarityjoins.datastructure.Category
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
object PartEnum {
  private val MAX_LEN = 3300 // the max size of the candidate string
  private val MAX_CATEGORY = 100 // max slice of the string, usually won't reach this limit
  def getCategories(threshold: Double, maxLen: Int): ArrayBuffer[Category] = {
    val categories = ArrayBuffer[Category]()
    var len = 1
    breakable {
      for (k <- 0 until MAX_CATEGORY) {
        categories += new Category(len, threshold)
        len = categories(k).e_len + 1
        if (len > maxLen)
          break
      }
    }
    categories
  }
  def tokenize(documents: RDD[(Int, String)]): RDD[(Int, String, Array[String])] = {
    documents.map(t => (t._1, t._2, t._2.toLowerCase.split("[\\W_]").filter(!_.isEmpty)))
  }
  def sizeBasedFiltering(tokens: RDD[(Int, String, Array[String])], categories: ArrayBuffer[Category]): RDD[(Int, Iterable[(Int, Int, String, Array[String])])] = {
    val categoryAndTokens: RDD[(Int, Int, String, Array[String])] = tokens.map(token => {
      var categoryIndex = -1
      breakable {
        for (i <- 0 until categories.length) {
          val helper = categories(i)
          if (token._3.length >= helper.s_len && token._3.length <= helper.e_len) {
            categoryIndex = i
            break
          }
        }
      }
      (categoryIndex, token._1, token._2, token._3)
    })
    categoryAndTokens.groupBy(_._1).leftOuterJoin(categoryAndTokens.map(t => (t._1 - 1, t._2, t._3, t._4)).groupBy(_._1))
      .map(t => {
        val tokens: Iterable[(Int, Int, String, Array[String])] = t._2._1
        val anotherTokens: Iterable[(Int, Int, String, Array[String])] = t._2._2.orNull
        val mergedTokens = new ArrayBuffer[(Int, Int, String, Array[String])]()
        tokens.foreach(mergedTokens += _)
        if (anotherTokens != null) {
          anotherTokens.foreach(mergedTokens += _)
        }
        (t._1, mergedTokens.distinct)
      })
  }
  def getCandidates(documents: RDD[(Int, String)], threshold: Double): RDD[((Int, Int, String, Array[String]), (Int, Int, String, Array[String]))] = {
    val tokens = tokenize(documents)
    val categories: ArrayBuffer[Category] = getCategories(threshold, MAX_LEN)
    val groupedCandidates = sizeBasedFiltering(tokens, categories)
    groupedCandidates.join(groupedCandidates).flatMap(t => {
      var count = -1l
      t._2._1.map(token => {
        count += 1
        t._2._2.zipWithIndex.filter(_._2 > count).map(_._1).map(anotherToken => {
          if (token._2 != anotherToken._2) (token, anotherToken) else null
        }).filter(_ != null)
      }).flatten
    })
  }
  def getMatches(documents: RDD[(Int, String)], threshold: Double): RDD[(Int, Int, Double)] = {
    val candidates = getCandidates(documents, threshold)
    candidates.map(t => (t._1._2, t._2._2, MatchingFunctions.jaccardSimilarity(KeyValue(null, t._1._3), KeyValue(null, t._2._3)))).filter(t => t._3 >= threshold)
  }
}
