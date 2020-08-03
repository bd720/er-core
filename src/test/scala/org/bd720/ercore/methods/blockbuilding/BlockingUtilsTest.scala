package org.wumiguo.ser.methods.blockbuilding
import org.scalatest.FlatSpec
class BlockingUtilsTest extends FlatSpec {
  it should "(Int,Seq) to Iterable(tuple)" in {
    val profileEntryKeys = (1, Seq("welcome", "to", "ER", "spark", "ER"))
    val out = BlockingUtils.associateKeysToProfileID(profileEntryKeys)
    assert(out.size == 5)
    out.foreach(t => println("data1 is " + t._1 + " , " + t._2))
    val sorted = out.toList.sortBy(_._1)
    assertResult(sorted.head)(("ER", 1))
  }
  it should "(Int,Seq) to Iterable(String,VectorOfLetter)" in {
    val profileEntryKeys = (1, Seq("welcome", "ER", "spark", "ER"))
    val out = BlockingUtils.associateKeysToProfileIdEntropy(profileEntryKeys)
    assert(out.size == 4)
    out.foreach(t => println("data2 is " + t._1 + " , " + t._2))
    var sorted = out.toList.sortBy(_._1)
    println(sorted)
    assertResult(sorted.head)(("ER", (1, Vector(69, 82))))
  }
}
