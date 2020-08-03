package org.wumiguo.ser.dataloader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.datastructure.{KeyValue, MatchingEntities, Profile}
import org.wumiguo.ser.testutil.TestDirs
import scala.collection.mutable
class WrapperTraitTest extends FlatSpec
  with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "test row to attributes " in {
    val columnNames = Array("staffId", "name", "role","gender")
    val testDfInputCsv = TestDirs.resolveTestResourcePath("data/csv/sample-dataframe.csv")
    val result = SimpleWrapperTrait.forInnerSplitTest(testDfInputCsv, columnNames)
    val firstSorted = result.first().sortBy(_.key)
    assertResult(mutable.MutableList(KeyValue("gender", "f"), KeyValue("name", "gem deng"),
      KeyValue("role", "singer"),
      KeyValue("staffId", "43676354")))(firstSorted)
  }
  it should "test row to attributes with inner split " in {
    val columnNames = Array("staffId", "name", "role", "gender", "songStyles", "otherInfo")
    val testDfInputCsv = TestDirs.resolveTestResourcePath("data/csv/sample-dataframe-innersplit.csv")
    val result = SimpleWrapperTrait.forInnerSplitTest(testDfInputCsv, columnNames)
    val firstSorted = result.first().sortBy(_.key)
    assertResult(mutable.MutableList(KeyValue("gender", "f"), KeyValue("name", "gem deng"),
      KeyValue("role", "singer"),
      KeyValue("songStyles","rap"), KeyValue("songStyles","pop"),  KeyValue("songStyles","english"),
      KeyValue("staffId", "43676354")))(firstSorted)
  }
  object SimpleWrapperTrait extends WrapperTrait with Serializable {
    def forTest(testDfInputCsv: String, columnNames: Array[String]) = {
      val sparkSession = SparkSession.builder().getOrCreate()
      val df = sparkSession.read.option("header", true).option("sep", ",").option("delimiter", "\"").csv(testDfInputCsv)
      val result = df.rdd.map(row => rowToAttributes(columnNames, row, false, ""))
      result
    }
    def forInnerSplitTest(testDfInputCsv: String, columnNames: Array[String]) = {
      val sparkSession = SparkSession.builder().getOrCreate()
      val df = sparkSession.read.option("header", true).option("sep", ",").option("delimiter", "\"").csv(testDfInputCsv)
      val result = df.rdd.map(row => rowToAttributes(columnNames, row, true, ";"))
      result
    }
    override def loadProfiles(filePath: String, startIDFrom: Int, realIDField: String, sourceId: Int): RDD[Profile] = ???
    override def loadGroundtruth(filePath: String): RDD[MatchingEntities] = ???
  }
}
