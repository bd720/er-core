package org.wumiguo.ser.methods.entityclustering
import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile, UnweightedEdge, WeightedEdge}
import scala.collection.mutable
class ConnectedComponentsClusteringTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "getClusters v1" in {
    val candiPairs = spark.sparkContext.parallelize(Seq(WeightedEdge(11, 12, 0.6), WeightedEdge(12, 1111, 0.4), WeightedEdge(11, 1111, 0.1)))
    val attrs = mutable.MutableList[KeyValue]()
    attrs += KeyValue("title", "entity matching test")
    val prof1 = Profile(11, attrs, "o101", 11)
    val prof2 = Profile(12, attrs, "o101", 12)
    attrs += KeyValue("author", "levin")
    val prof3 = Profile(11111, attrs, "o111", 11211)
    val profiles = spark.sparkContext.parallelize(Seq(prof1, prof2, prof3), 2)
    val ccc1 = ConnectedComponentsClustering.getClusters(profiles, candiPairs, 0, 0.3)
    ccc1.foreach(x => println("connected1=" + x))
    val similarPairs1 = ccc1.sortBy(_._2.size, false).first()
    assert(2 == ccc1.count())
    assertResult((0, Set(11, 12, 1111)))(similarPairs1)
    val ccc2 = ConnectedComponentsClustering.getClusters(profiles, candiPairs, 0, 0.5)
    ccc2.foreach(x => println("connected2=" + x))
    val similarPairs2 = ccc2.sortBy(_._2.size, false).first()
    assert(2 == ccc2.count())
    assertResult((0, Set(11, 12)))(similarPairs2)
  }
}
