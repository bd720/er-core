package org.wumiguo.ser.dataloader
import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.datastructure.{KeyValue, MatchingEntities, Profile}
import org.wumiguo.ser.testutil.TestDirs
import scala.collection.mutable
class CSVProfileLoaderTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(this.getClass.getName)
  it should "load ground truth with header" in {
    val gtPath = TestDirs.resolveTestResourcePath("data/csv/dblpAcmIdDuplicates-withheader.csv")
    val meRdd = CSVProfileLoader.loadGroundTruth(gtPath, ",", true)
    assert(meRdd != null)
    assert(meRdd.count() == 200)
    val first = meRdd.sortBy(_.firstEntityID, ascending = true).first()
    assertResult(MatchingEntities("1", "1093"))(first)
  }
  it should "load ground truth with header and take header as normal entry" in {
    val gtPath = TestDirs.resolveTestResourcePath("data/csv/dblpAcmIdDuplicates-withheader.csv")
    val meRdd = CSVProfileLoader.loadGroundTruth(gtPath, ",", false)
    assert(meRdd != null)
    assert(meRdd.count() == 201)
    val first = meRdd.sortBy(_.firstEntityID, ascending = false).first()
    assertResult(MatchingEntities("entityId1", "entityId2"))(first)
  }
  it should "load valid ground truth" in {
    val gtPath = TestDirs.resolveTestResourcePath("data/csv/dblpAcmIdDuplicates-noheader.gen.csv")
    val meRdd = CSVProfileLoader.loadGroundTruth(gtPath)
    assert(meRdd != null)
    assert(meRdd.count() == 3)
    meRdd.foreach(e => println(e))
    val first = meRdd.first()
    assertResult(MatchingEntities("1821", "1345"))(first)
  }
  it should "load 10 entity profiles" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.10.csv")
    val startIdFrom = 100
    val realIDField = ""
    val ep1Rdd = CSVProfileLoader.loadProfiles(ep1Path, startIdFrom, realIDField)
    assert(10 == ep1Rdd.count())
    assertResult(Profile(100, mutable.MutableList(
      KeyValue("_c0", "0"),
      KeyValue("_c1", "The WASA2 object-oriented workflow management system"),
      KeyValue("_c2", "International Conference on Management of Data"),
      KeyValue("_c3", "Gottfried Vossen, Mathias Weske")), "", 0)
    )(ep1Rdd.first())
  }
  it should "load 15 entity profiles with header" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.h.15.csv")
    val startIdFrom = 1
    val ep1Rdd = CSVProfileLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom, separator = ",", header = true)
    println("ep1Rdd " + ep1Rdd.count())
    ep1Rdd.foreach(x => println("epx " + x))
    assert(15 == ep1Rdd.count())
    assertResult(Profile(1, mutable.MutableList(
      KeyValue("year", "0"),
      KeyValue("title", "The WASA2 object-oriented workflow management system"),
      KeyValue("venue", "International Conference on Management of Data"),
      KeyValue("authors", "Gottfried Vossen, Mathias Weske")), "", 0)
    )(ep1Rdd.first())
  }
  it should "load 15 entity profiles with header and id column" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.h.15.csv")
    val startIdFrom = 1
    val ep1Rdd = CSVProfileLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom, separator = ",", header = true, realIDField = "year")
    println("ep1Rdd " + ep1Rdd.count())
    ep1Rdd.foreach(x => println("epx " + x))
    assert(15 == ep1Rdd.count())
    assertResult(Profile(1, mutable.MutableList(
      KeyValue("title", "The WASA2 object-oriented workflow management system"),
      KeyValue("venue", "International Conference on Management of Data"),
      KeyValue("authors", "Gottfried Vossen, Mathias Weske")), "0", 0)
    )(ep1Rdd.first())
  }
}
