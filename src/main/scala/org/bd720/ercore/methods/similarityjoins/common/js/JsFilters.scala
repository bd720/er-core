package org.bd720.ercore.methods.similarityjoins.common.js
object JsFilters {
  def getPrefix(tokens: Array[Int], threshold: Double, k: Int = 1): Array[Int] = {
    val len = tokens.length
    tokens.take(len - Math.ceil(len.toDouble * threshold).toInt + k)
  }
  def positionFilter(lenDoc1: Int, lenDoc2: Int, posDoc1: Int, posDoc2: Int, o: Int, threshold: BigDecimal): Boolean = {
    val alpha = Math.ceil((threshold * (lenDoc1 + lenDoc2) / (1 + threshold)).toDouble).toInt
    (o + Math.min(lenDoc1 - posDoc1, lenDoc2 - posDoc2)) >= alpha
  }
  def getPrefixLength(docLen: Int, threshold: Double, k: Int = 1) : Int = {
    docLen - Math.ceil(docLen.toDouble * threshold).toInt + 1
  }
}
