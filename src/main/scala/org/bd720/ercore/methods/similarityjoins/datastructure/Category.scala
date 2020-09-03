package org.bd720.ercore.methods.similarityjoins.datastructure
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
class Category extends Serializable {
  var THRESHOLD: Double = 0.000001f 
  var N1: Int = 0
  var N2: Int = 0
  var K: Int = 0
  var K2: Int = 0
  var range_start: ArrayBuffer[ArrayBuffer[Int]] = new ArrayBuffer[ArrayBuffer[Int]]()
  var range_end: ArrayBuffer[ArrayBuffer[Int]] = new ArrayBuffer[ArrayBuffer[Int]]()
  var s_len = 0
  var e_len = 0
  var sig_len = 0
  var subs: ArrayBuffer[ArrayBuffer[Int]] = new ArrayBuffer[ArrayBuffer[Int]]
  var sig_map: Array[mutable.HashMap[Integer, ArrayBuffer[Int]]] = null
  def this(len: Int, threshold: Double) {
    this()
    THRESHOLD = threshold
    s_len = len 
    e_len = (s_len / THRESHOLD).toInt 
    K = (2 * (1 - THRESHOLD) / (1 + THRESHOLD) * e_len.toDouble).toInt
    N1 = K + 1
    N2 = 2
    K2 = (K + 1) / N1 - 1
    if ((K + 1) % N1 != 0) {
      K2 += 1
    }
    if (N1 > K + 1 || N1 * N2 <= K + 1) {
      throw new RuntimeException("N1 * N2 is not more than K + 1,consider to adjust the N1 and N2")
    }
  }
}
