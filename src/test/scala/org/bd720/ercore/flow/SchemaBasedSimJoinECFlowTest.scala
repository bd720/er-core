package org.bd720.ercore.flow
import org.scalatest.flatspec.AnyFlatSpec
import org.bd720.ercore.common.SparkEnvSetup
class SchemaBasedSimJoinECFlowTest extends AnyFlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "mapMatchesWithProfilesAndSimilarity" in {
  }
}
