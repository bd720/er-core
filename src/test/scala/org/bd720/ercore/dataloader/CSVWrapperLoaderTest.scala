package org.bd720.ercore.dataloader
import org.scalatest.FlatSpec
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.testutil.TestDirs
class CSVWrapperLoaderTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should " be able to load good entity profiles csv" in {
    val testCsv = TestDirs.resolveDataPath("csv/sampleEP1.csv")
    val startIDFrom = 0
    val realIDField = "entityId1"
    val rdd = CSVWrapper.loadProfiles(testCsv, startIDFrom, realIDField)
    assert(rdd != null)
  }
}
