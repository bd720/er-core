package org.bd720.ercore.methods.similarityjoins.common.js
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.datastructure.Profile
import org.bd720.ercore.methods.similarityjoins.datastructure.TokenDocumentInfo
object CommonJsFunctions {
  def tokenize(document: String): Set[String] = {
    document.split("[\\W_]").map(_.trim.toLowerCase).filter(_.length > 0).toSet 
  }
  def prepareData(profiles: RDD[Profile], fields: List[String] = List.empty[String]): RDD[(Int, Array[Int])] = {
    val docs = profiles.map { p =>
      val data = {
        if (fields.isEmpty) {
          p.attributes.map(_.value).mkString(" ")
        }
        else {
          p.attributes.filter(a => fields.contains(a.key)).map(_.value).mkString(" ")
        }
      }
      (p.id, data)
    }
    tokenizeAndSort(docs)
  }
  def tokenizeAndSort(documents: RDD[(Int, String)]): RDD[(Int, Array[Int])] = {
    val tokenizedDocuments = documents.map { case (documentId, valueString) => (documentId, tokenize(valueString)) }
    val tekenCount = computeTokenCount(tokenizedDocuments)
    val sc = documents.context
    val tekenCount_broadcast: Broadcast[scala.collection.Map[String, (Double, Int)]] = sc.broadcast(tekenCount.collectAsMap())
    sortTokens(tokenizedDocuments, tekenCount_broadcast)
  }
  def computeTokenCount(tokenizedDocuments: RDD[(Int, Set[String])]): RDD[(String, (Double, Int))] = {
    def create(e: Double): Double = e
    def add(acc: Double, e: Double): Double = acc + e
    def merge(acc1: Double, acc2: Double): Double = acc1 + acc2
    tokenizedDocuments
      .flatMap { case (documentId, valueSet) => valueSet.map { token => (token, 1.0) } } 
      .combineByKey(create, add, merge) 
      .sortBy(x => (x._2, x._1)) 
      .zipWithIndex() 
      .map { case ((token, tokenCount), tokenId) => (token, (tokenCount, tokenId.toInt)) }
  }
  def sortTokens(tokenizedDocuments: RDD[(Int, Set[String])],
                 tekenCount_broadcast: Broadcast[scala.collection.Map[String, (Double, Int)]]): RDD[(Int, Array[Int])] = {
    tokenizedDocuments.map { case (documentID, valueSet) =>
      val sortedTokenIDs = valueSet.toArray.map {
        token =>
          val tokenCountId = tekenCount_broadcast.value(token)
          tokenCountId._2
      }.sorted 
      (documentID, sortedTokenIDs)
    }
  }
  def buildPrefixIndex(tokenizedDocOrd: RDD[(Int, Array[Int])], threshold: Double): RDD[(Int, Array[TokenDocumentInfo])] = {
    val indices = tokenizedDocOrd.flatMap {
      case (docId, tokens) =>
        val prefixTokens = JsFilters.getPrefix(tokens, threshold)
        prefixTokens.zipWithIndex.map {
          case (prefixTokenId, pos) =>
            (prefixTokenId, new TokenDocumentInfo(docId, pos + 1, tokens.length))
        }
    }
    indices.groupByKey()
      .filter(_._2.size > 1)
      .map { case (prefixTokenId, listDocId_tokenPos_numTokens) =>
        (prefixTokenId, listDocId_tokenPos_numTokens.toArray.sortBy(_.docLen))
      }
  }
  def passJS(doc1: Array[Int], doc2: Array[Int], threshold: Double): Boolean = {
    val common = doc1.intersect(doc2).length
    (common.toDouble / (doc1.length + doc2.length - common)) >= threshold
  }
}
