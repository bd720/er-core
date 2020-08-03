package org.bd720.ercore.methods.entitymatching
import org.scalatest.FlatSpec
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.methods.datastructure.{KeyValue, Profile, UnweightedEdge, WeightedEdge}
import scala.collection.mutable
class EntityMatchingTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "getComparisons " in {
    val candiPairs = spark.sparkContext.parallelize(Seq(UnweightedEdge(11, 101), UnweightedEdge(12, 102), UnweightedEdge(11, 111)))
    val attrs = mutable.MutableList[KeyValue]()
    attrs += KeyValue("title", "entity matching test")
    val prof1 = Profile(11, attrs, "o101", 11)
    val prof2 = Profile(12, attrs, "o101", 12)
    val profiles = spark.sparkContext.parallelize(Seq((11, prof1), (12, prof2)))
    val data = EntityMatching.getComparisons(candiPairs, profiles)
    val first = data.sortBy(_._1.id).first()
    assertResult(prof1)(first._1)
    assertResult(List(101, 111))(first._2)
  }
  it should "getComparisons with error " in {
    var hasError = false
    try {
      val candiPairs = spark.sparkContext.parallelize(Seq(UnweightedEdge(11, 101), UnweightedEdge(12, 102), UnweightedEdge(11, 111)))
      val attrs = mutable.MutableList[KeyValue]()
      attrs += KeyValue("title", "entity matching test")
      val profiles = spark.sparkContext.parallelize(Seq((11, Profile(11, attrs, "o101", 11))))
      val data = EntityMatching.getComparisons(candiPairs, profiles)
      println("result:" + data.count())
      data.foreach(x => println("data:" + x._1 + ", " + x._2.toList))
    } catch {
      case e: Throwable =>
        hasError = true
        println("expect error :" + e)
        assert(e.getMessage.contains("NoSuchElementException: None.get"))
    }
    assert(hasError)
  }
  it should "group linkage" in {
    var attrs1 = mutable.MutableList[KeyValue](KeyValue("title", "how to write java program"), KeyValue("author", "levin"))
    var attrs2 = mutable.MutableList[KeyValue](KeyValue("title", "how to write java code"), KeyValue("author", "levin"))
    val p1 = Profile(111, attrs1, "jaOkd", 100)
    val p2 = Profile(222, attrs2, "Uja2d", 102)
    val weightedEdge1 = EntityMatching.groupLinkage(p1, p2, 0.8)
    println("we1:" + weightedEdge1)
    val weightedEdge2 = EntityMatching.groupLinkage(p1, p2)
    assert(weightedEdge2.weight > weightedEdge1.weight)
    println("we2:" + weightedEdge2)
    val weightedEdge3 = EntityMatching.groupLinkage(p1, p2, 0.1)
    println("we3:" + weightedEdge3)
    assert(weightedEdge3.weight >= weightedEdge2.weight)
    var attrs3 = mutable.MutableList[KeyValue](KeyValue("title", "how to write code java"), KeyValue("author", "levin"))
    var attrs4 = mutable.MutableList[KeyValue](KeyValue("title", "how to write java code"), KeyValue("author", "levin"))
    val p3 = Profile(333, attrs3, "jdaOkd", 100)
    val p4 = Profile(444, attrs4, "Uxal2d", 102)
    val weightedEdge4 = EntityMatching.groupLinkage(p3, p4)
    assertResult(WeightedEdge(333, 444, 1.0))(weightedEdge4)
    var attrs5 = mutable.MutableList[KeyValue](KeyValue("title", "nothing in common"))
    var attrs6 = mutable.MutableList[KeyValue](KeyValue("title", "similarity will be zero"))
    val p5 = Profile(555, attrs5, "jdaOkd", 150)
    val p6 = Profile(666, attrs6, "Uxal2d", 162)
    val weightedEdge5 = EntityMatching.groupLinkage(p5, p6)
    assertResult(WeightedEdge(555, 666, -1.0))(weightedEdge5)
  }
  it should "getProfilesMap" in {
    val profRdd = spark.sparkContext.parallelize(
      Seq(
        (1, Profile(111, mutable.MutableList[KeyValue](KeyValue("title", "first article")), "jdaOkd", 151)),
        (2, Profile(2222, mutable.MutableList[KeyValue](KeyValue("title", "2nd post")), "axk11ld", 152)),
        (3, Profile(333, mutable.MutableList[KeyValue](KeyValue("title", "3rd post")), "yagk10x", 323)),
        (4, Profile(444, mutable.MutableList[KeyValue](KeyValue("title", "4th post")), "Jzla29K", 323)),
        (5, Profile(555, mutable.MutableList[KeyValue](KeyValue("title", "5th post")), "Jzla29K", 323))
      ), 2   val mapRdd = EntityMatching.getProfilesMap(profRdd)
    mapRdd.foreach(x => println("map:" + x))
    assert(mapRdd.count() == 2)
    val part = mapRdd.filter(_.contains(1)).collect().toList.head
    assertResult(Profile(111, mutable.MutableList[KeyValue](KeyValue("title", "first article")), "jdaOkd", 151))(part.get(1).get)
  }
  it should "profileMatching" in {
    var attrs1 = mutable.MutableList[KeyValue](KeyValue("title", "how to write java program"), KeyValue("author", "levin"))
    var attrs2 = mutable.MutableList[KeyValue](KeyValue("title", "how to write java code"), KeyValue("author", "levin"))
    val p1 = Profile(111, attrs1, "jaOkd", 100)
    val p2 = Profile(222, attrs2, "Uja2d", 102)
    val weightedEdge = EntityMatching.profileMatching(p1, p2, MatchingFunctions.jaccardSimilarity)
    println("we:" + weightedEdge)
    assertResult(WeightedEdge(111, 222, 0.7142857142857143))(weightedEdge)
    val weightedEdge2 = EntityMatching.profileMatching(p1, p2, MatchingFunctions.enhancedJaccardSimilarity)
    println("we:" + weightedEdge2)
    assertResult(WeightedEdge(111, 222, 0.7142857142857143))(weightedEdge2)
  }
  it should "has error on entityMatchingCB with default partition number " in {
    val candiPairs = spark.sparkContext.parallelize(Seq(UnweightedEdge(11, 101), UnweightedEdge(12, 102), UnweightedEdge(11, 111)))
    val attrs = mutable.MutableList[KeyValue]()
    attrs += KeyValue("title", "entity matching test")
    val prof1 = Profile(11, attrs, "o101", 11)
    val prof2 = Profile(12, attrs, "o101", 12)
    try {
      val profiles = spark.sparkContext.parallelize(Seq(prof1, prof2), 4)
      EntityMatching.entityMatchingCB(profiles, candiPairs, 2)
      assert(false, "should have error on above code")
    } catch {
      case e: Throwable =>
        assert(e.getMessage.contains("UnsupportedOperationException: empty.reduceLeft"))
    }
  }
  it should "entityMatchingCB" in {
    val candiPairs = spark.sparkContext.parallelize(Seq(UnweightedEdge(11, 101), UnweightedEdge(11111, 11), UnweightedEdge(12, 102), UnweightedEdge(11, 111)))
    val attrs = mutable.MutableList[KeyValue]()
    attrs += KeyValue("title", "entity matching test")
    val prof1 = Profile(11, attrs, "o101", 11)
    val prof2 = Profile(12, attrs, "o101", 12)
    attrs += KeyValue("author", "levin")
    val prof3 = Profile(11111, attrs, "o111", 11211)
    val profiles = spark.sparkContext.parallelize(Seq(prof1, prof2, prof3), 2)
    val weightedEdgeWithRdd = EntityMatching.entityMatchingCB(profiles, candiPairs, 2)
    println("x=:" + weightedEdgeWithRdd._2)
    val weRdd = weightedEdgeWithRdd._1
    println("count=" + weRdd.count())
    weRdd.foreach(x => println("weRdd=" + x))
  }
}
