package org.bd720.ercore.methods.blockbuilding
import org.scalatest.FlatSpec
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.dataloader.String2Profile
import org.bd720.ercore.methods.datastructure.{KeyValue, Profile}
import org.bd720.ercore.testutil.TestDirs
import scala.collection.mutable
class SortedNeighborhoodTest
  extends FlatSpec
    with SparkEnvSetup
    with Serializable {
  implicit val spark = createLocalSparkSession(this.getClass.getName)
  it should "create blocks " in {
    import spark.implicits._
    val rddTextFilePath = TestDirs.resolveDataPath("/text/profile1Sample.txt")
    val profileRaw = spark.sparkContext.textFile(rddTextFilePath)
    profileRaw.foreach(x => println("raw " + x))
    val fmtProfile = profileRaw.map(x => String2Profile.string2Profile(x))
    val winSize = 3
    val result = SortedNeighborhood.createBlocks(fmtProfile, winSize)
    assert(result != null)
    result.foreach(b => println("result entry is " + b))
    println(result.id + " name " + result.name)
  }
}
