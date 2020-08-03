package org.bd720.ercore.methods.similarityjoins
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.methods.similarityjoins.common.ed.CommonEdFunctions
import org.bd720.ercore.methods.similarityjoins.simjoin.EDJoin
class EDJoinTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "getPositionalQGrams with q=2" in {
    val qgram2 = EDJoin.getPositionalQGrams(spark.sparkContext.parallelize(Seq((0, "abcd"))), 2).collect
    assertResult(
      Array(("ab", 0), ("bc", 1), ("cd", 2))
    )(qgram2(0)._3)
  }
  it should "getPositionalQGrams with q=3" in {
    val qgram3 = EDJoin.getPositionalQGrams(spark.sparkContext.parallelize(Seq((0, "abcd"))), 3).collect
    assertResult(
      Array(("abc", 0), ("bcd", 1))
    )(qgram3(0)._3)
  }
  it should "buildPrefixIndex will group string by token" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (0, "abcd", Array(("ab", 0), ("bc", 1), ("cd", 2))),
        (1, "abbd", Array(("ab", 0), ("bb", 1), ("bd", 2))),
        (2, "abcc", Array(("ab", 0), ("bc", 1), ("cc", 2)))
      ))
    val sortedDocs = CommonEdFunctions.getSortedQgrams2(docs)
    val prefixIndex = EDJoin.buildPrefixIndex(sortedDocs, 2, 1).collect
    assertResult(
      Array("abcd", "abcc")
    )(prefixIndex(0)._2.map(_._4))
    assertResult(
      Array("abcd", "abbd", "abcc")
    )(prefixIndex(1)._2.map(_._4))
  }
  it should "buildPrefixIndex will filter value habce since there is 2 change" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (0, "abcd", Array(("ab", 0), ("bc", 1), ("cd", 2))),
        (1, "habce", Array(("ha", 0), ("ab", 1), ("bc", 2), ("ce", 3))),
        (2, "habcd", Array(("ha", 0), ("ab", 1), ("bc", 2), ("cd", 3)))
      ))
    val sortedDocs = CommonEdFunctions.getSortedQgrams2(docs)
    val prefixIndex = EDJoin.buildPrefixIndex(sortedDocs, 2, 1).collect
    assertResult(
      Array("abcd", "habcd")
    )(prefixIndex(0)._2.map(_._4))
  }
  it should "getMatches should match string within edit distance is 1" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, "this string with 1 insert change"),
        (2, "mthis string with 1 insert change"),
        (3, "this string with 1 substitution change"),
        (4, "mhis string with 1 substitution change"),
        (5, "this string with 1 delete change"),
        (6, "his string with 1 delete change"),
        (7, "first"),
        (8, "second")
      ))
    val results = EDJoin.getMatches(docs, 3, 1).collect
    assertResult(
      Array((1, 2, 1.0), (3, 4, 1.0), (5, 6, 1.0))
    )(results.sortBy(_._1))
  }
  it should "getMatches should not match string within edit distance is 2 when the threadhold is 1" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, "this string with 2 insert change"),
        (2, "mmthis string with 2 insert change"),
        (3, "this string with 2 substitution change"),
        (4, "mmis string with 2 substitution change"),
        (5, "this string with 2 delete change"),
        (6, "is string with 2 delete change")
      ))
    val results = EDJoin.getMatches(docs, 3, 1).collect
    assertResult(Array())(results)
  }
  it should "getCandidates" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, "this string with 1 insert change"),
        (2, "mthis string with 1 insert change"),
        (3, "this string with 1 substitution change"),
        (4, "mhis string with 1 substitution change"),
        (5, "this string with 1 delete change"),
        (6, "his string with 1 delete change")
      ))
    val candis = EDJoin.getCandidates(docs, 3, 1)
    candis.foreach(x => println("candi=" + x))
    assertResult(3)(candis.count())
    assertResult(
      ((1, "this string with 1 insert change"), (2, "mthis string with 1 insert change"))
    )(candis.sortBy(_._1._1).first())
  }
  it should "getCandidatePairs" in {
    val prefixIndex = spark.sparkContext.parallelize(
      Seq[(Int, Arrayt, Int, Array[(Int, Int)], String)])](
        (1, Array((1, 1, Array((1, 1)), ""))),
        (2, Array((1, 1, Array((1, 1)), "")))
      )
    )
    val qgramLength = 2
    val threshold = 2
    val pairRdd = EDJoin.getCandidatePairs(prefixIndex, qgramLength, threshold)
    println("pairCount=" + pairRdd.count())
    pairRdd.foreach(x => println("pair=" + x))
  }
}
