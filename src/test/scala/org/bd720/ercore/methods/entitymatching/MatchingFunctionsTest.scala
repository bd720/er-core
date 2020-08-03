package org.bd720.ercore.methods.entitymatching
import org.scalatest.FlatSpec
import org.bd720.ercore.methods.datastructure.{KeyValue, Profile}
import org.bd720.ercore.methods.entitymatching.MatchingFunctions.jaccardSimilarity
import scala.collection.mutable
class MatchingFunctionsTest extends FlatSpec {
  it should "get jaccardSimilarity " in {
    var similarity = jaccardSimilarity(KeyValue("title", "testing code"), KeyValue("title", "testing code"))
    println(similarity)
    assertResult(1.0)(similarity)
    similarity = jaccardSimilarity(KeyValue("title", "TESTING code"), KeyValue("title", "testing code"))
    assertResult(1.0)(similarity)
    similarity = jaccardSimilarity(KeyValue("article", "TESTING code"), KeyValue("title", "testing code"))
    assertResult(1.0)(similarity)
    similarity = jaccardSimilarity(KeyValue("title", "code testing"), KeyValue("title", "testing code"))
    assertResult(1.0)(similarity)
    similarity = jaccardSimilarity(KeyValue("title", "write code testing"), KeyValue("title", "testing write code"))
    assertResult(1.0)(similarity)
    similarity = jaccardSimilarity(KeyValue("title", "just do unit-testing"), KeyValue("title", "testing code"))
    println("jaccardSimilarity: " + similarity)
    assert(1.0 > similarity && similarity > 0.1)
    similarity = jaccardSimilarity(KeyValue("title", "just do function verification"), KeyValue("title", "testing code"))
    println("jaccardSimilarity: " + similarity)
    assert(0 == similarity)
  }
  it should " getSimilarityEdges " in {
    val attrs1 = mutable.MutableList[KeyValue](KeyValue("title", "helloworld"), KeyValue("author", "lev"))
    val attrs2 = mutable.MutableList[KeyValue](KeyValue("title", "hello world"), KeyValue("author", "liu"))
    val p1 = Profile(1, attrs1, "jaOkd", 100)
    val p2 = Profile(2, attrs2, "Uja2d", 102)
    val priorityQueue = MatchingFunctions.getSimilarityEdges(p1, p2)
    println("pq is " + priorityQueue)
    assertResult(0)(priorityQueue.size)
    val priorityQueue1 = MatchingFunctions.getSimilarityEdges(p1, p2, 0.1)
    println("pq1 is " + priorityQueue1)
    assertResult(0)(priorityQueue1.size)
    val attrs3 = mutable.MutableList[KeyValue](KeyValue("title", "how to write some code"), KeyValue("auth", "lev"))
    val attrs4 = mutable.MutableList[KeyValue](KeyValue("title", "step on write some code"), KeyValue("auth", "lev liu"))
    val p3 = Profile(1, attrs3, "jaOkd", 100)
    val p4 = Profile(2, attrs4, "Uja2d", 102)
    val priorityQueue2 = MatchingFunctions.getSimilarityEdges(p3, p4, 0.8)
    println("pq2 is " + priorityQueue2)
    assertResult(0)(priorityQueue2.size)
    val priorityQueue3 = MatchingFunctions.getSimilarityEdges(p3, p4)
    println("pq3 is " + priorityQueue3)
    assertResult(0)(priorityQueue3.size)
    val priorityQueue4 = MatchingFunctions.getSimilarityEdges(p3, p4, 0.3)
    println("pq4 is " + priorityQueue4)
    assertResult(2)(priorityQueue4.size)
    assertResult((0.5, ("a1", "b1")))(priorityQueue4.head)
  }
  it should "getCharactersFrequency" in {
    val attrs1 = mutable.MutableList[KeyValue](
      KeyValue("title", "let's start to write some java code"),
      KeyValue("year", "2020"),
      KeyValue("abs", "simple coding course"),
      KeyValue("tag", "java code develop"),
      KeyValue("category", "engineering & code"),
      KeyValue("auth", "lev"))
    val p1 = Profile(1, attrs1, "jaOkd", 100)
    val resultMap = MatchingFunctions.getCharactersFrequency(p1, 2)
    val sorted = resultMap.toSeq.sortWith(_._2 > _._2)
    println("result map :" + resultMap)
    println("sorted : " + sorted)
    assertResult(("co", 5))(sorted.head)
    val resultMap2 = MatchingFunctions.getCharactersFrequency(p1, 4)
    val sorted2 = resultMap2.toSeq.sortWith(_._2 > _._2)
    println("result map :" + resultMap2)
    println("sorted : " + sorted2)
    assertResult(("code", 3))(sorted2.head)
  }
  it should "getNGramsFrequency" in {
    val attrs1 = mutable.MutableList[KeyValue](
      KeyValue("title", "let's start to write some java code"),
      KeyValue("year", "2020"),
      KeyValue("ab "simple coding course"),
      KeyValue("tag", "java code develop"),
      KeyValue("category", "engineering & code"),
      KeyValue("auth", "lev"))
    val p1 = Profile(1, attrs1, "jaOkd", 100)
    val resultMap = MatchingFunctions.getNGramsFrequency(p1, 2)
    val sorted = resultMap.toSeq.sortWith(_._2 > _._2)
    println("result map :" + resultMap)
    println("sorted : " + sorted)
    assertResult(("java code", 2))(sorted.head)
    val resultMap2 = MatchingFunctions.getNGramsFrequency(p1, 1)
    val sorted2 = resultMap2.toSeq.sortWith(_._2 > _._2)
    println("result map :" + resultMap2)
    println("sorted : " + sorted2)
    assertResult(("code", 3))(sorted2.head)
  }
  it should "get cosine similarity " in {
    val attrs1 = mutable.MutableList[KeyValue](KeyValue("title", "helloworld"), KeyValue("author", "lev"))
    val attrs2 = mutable.MutableList[KeyValue](KeyValue("title", "hello world"), KeyValue("author", "liu"))
    val p1 = Profile(1, attrs1, "jaOkd", 100)
    val p2 = Profile(2, attrs2, "Uja2d", 102)
    val similarity1 = MatchingFunctions.chfCosineSimilarity(p1, p2)
    println("cosine sim: " + similarity1)
    val similarity2 = MatchingFunctions.chfCosineSimilarity(p1, p2, 4)
    println("cosine sim: " + similarity2)
    assert(similarity1 > similarity2, "less similar on larger token comparision")
    val attrs3 = mutable.MutableList[KeyValue](KeyValue("title", "how to write some code"), KeyValue("auth", "lev"))
    val attrs4 = mutable.MutableList[KeyValue](KeyValue("title", "step on write some code"), KeyValue("auth", "lev liu"))
    val p3 = Profile(1, attrs3, "jaOkd", 100)
    val p4 = Profile(2, attrs4, "Uja2d", 102)
    val similarity3 = MatchingFunctions.chfCosineSimilarity(p3, p4)
    println("cosine sim: " + similarity3)
    assert(similarity3 > 0, "it does have some letter-groups in common")
  }
}
