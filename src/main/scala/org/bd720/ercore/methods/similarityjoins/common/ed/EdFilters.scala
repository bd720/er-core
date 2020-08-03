package org.wumiguo.ser.methods.similarityjoins.common.ed
object EdFilters {
  def getPrefixLen(qGramLen: Int, threshold: Int): Int = {
    qGramLen * threshold + 1
  }
  def commonFilter(d1: Array[(Int, Int)], d2: Array[(Int, Int)], qgramLength: Int, threshold: Int): Boolean = {
    var pass = true
    val minCommon = (Math.max(d1.length, d2.length) - qgramLength + 1) - (qgramLength * threshold)
    if (minCommon > 0) {
      var i = 0
      var j = 0
      var common = 0
      var continue = true
      while (i < d1.length && j < d2.length && common < minCommon && continue) {
        if (d1(i)._1 < d2(j)._1) {
          i += 1
        }
        else if (d1(i)._1 > d2(j)._1) {
          j += 1
        }
        else {
          do {
            if (math.abs(d1(i)._2 - d2(j)._2) <= threshold) {
              common += 1
            }
            continue = (math.min(d1.length - i, d2.length - j) + common) >= minCommon
            if (d1(i)._2 < d2(j)._2) {
              i += 1
            }
            else if (d1(i)._2 > d2(j)._2) {
              j += 1
            }
            else {
              i += 1
              j += 1
            }
          } while (i < d1.length && j < d2.length && common < minCommon && continue && d1(i)._1 == d2(j)._1)
        }
      }
      pass = common >= minCommon
    }
    pass
  }
  def commonFilterAfterPrefix(d1: Array[(Int, Int)], d2: Array[(Int, Int)], qgramLength: Int, threshold: Int, commonPrefixQgrams: Int, d1StartPos: Int, d2StartPos: Int): Boolean = {
    var pass = true
    val minCommon = (Math.max(d1.length, d2.length) - qgramLength + 1) - (qgramLength * threshold)
    if (minCommon > 0) {
      var i = d1StartPos
      var j = d2StartPos
      var common = commonPrefixQgrams
      var continue = true
      while (i < d1.length && j < d2.length && common < minCommon && continue) {
        if (d1(i)._1 < d2(j)._1) {
          i += 1
        }
        else if (d1(i)._1 > d2(j)._1) {
          j += 1
        }
        else {
          do {
            if (math.abs(d1(i)._2 - d2(j)._2) <= threshold) {
              common += 1
            }
            continue = (math.min(d1.length - i, d2.length - j) + common) >= minCommon
            if (d1(i)._2 < d2(j)._2) {
              i += 1
            }
            else if (d1(i)._2 > d2(j)._2) {
              j += 1
            }
            else {
              i += 1
              j += 1
            }
          } while (i < d1.length && j < d2.length && common < minCommon && continue && d1(i)._1 == d2(j)._1)
        }
      }
      pass = common >= minCommon
    }
    pass
  }
}
