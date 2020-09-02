package org.bd720.ercore.datatransformer
import org.apache.spark.sql.AnalysisException
import org.scalatest.flatspec.AnyFlatSpec
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.dataloader.{DataTypeResolver, ProfileLoaderFactory}
import org.bd720.ercore.datatransformer.GenericDataTransformer.transform
import org.bd720.ercore.methods.datastructure.{KeyValue, Profile}
import org.bd720.ercore.testutil.TestDirs
import scala.collection.mutable
class GenericDataTransformerTest extends AnyFlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "fail to transform" in {
    val csvFile = "acmProfiles.10.csv"
    val dataFile = TestDirs.resolveDataPath("csv/" + csvFile)
    val joinResultFile = "acmP10_txf"
    val outputPath = TestDirs.resolveOutputPath("parquet")
    val outputType = "parquet"
    log.info("data file {}", dataFile)
    var hasError = false
    try {
      val finalOutputPath = transform(dataFile,
        toFolder = outputPath, toFileName = joinResultFile, toType = outputType,
        filteredFields = Nil, overwriteOnExist = true)
      assert(false, "Do expect to run here")
    }
    catch {
      case e: AnalysisException => {
        hasError = true
        e.printStackTrace()
      }
    }
    assert(hasError, "fail to transform csv when header (no header) contains special char like ' ' ")
  }
  it should "transform" in {
    val csvFile = "acmProfiles.h.15.v2.csv"
    val dataFile = TestDirs.resolveDataPath("csv/" + csvFile)
    val joinResultFile = "acmP15_v2_txf"
    val outputPath = TestDirs.resolveOutputPath("parquet")
    val outputType = "parquet"
    log.info("data file {}", dataFile)
    val finalOutputPath = transform(dataFile,
      toFolder = outputPath, toFileName = joinResultFile, toType = outputType,
      filteredFields = Nil, overwriteOnExist = true)
    assert(finalOutputPath != null)
    val outRdd = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(finalOutputPath))
      .load(finalOutputPath)
    log.info("first=" + outRdd.first())
  }
  it should "transform csv to parquet" in {
    val csvFile = "dt01.csv"
    val dataFile = TestDirs.resolveTestResourcePath("data/e2e/" + csvFile)
    val joinResultFile = "c2p_txf"
    val outputPath = TestDirs.resolveOutputPath("parquet")
    val outputType = "parquet"
    val idField = "t_id"
    val finalOutputPath = transform(dataFile,
      toFolder = outputPath, toFileName = joinResultFile, toType = outputType,
      idField = idField, filteredFields = Nil, overwriteOnExist = true)
    assertResult(outputPath + "/c2p_txf.parquet")(finalOutputPath)
    val outRdd = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(finalOutputPath))
      .load(finalOutputPath, realIDField = idField)
    assertResult(12)(outRdd.count())
    assertResult(
      Profile(0, mutable.MutableList(
        KeyValue("t_amount", "5020000"), KeyValue("t_user", "ll01"),
        KeyValue("t_date", "20200719"), KeyValue("t_pid", "P1007"), KeyValue("t_cptyid", "LC1291003001"),
        KeyValue("site", "CN"), KeyValue("system_id", "ALIBB")), "TCN001277", 0)
    )(outRdd.first())
  }
  it should "transform csv to parquet and keep id" in {
    val csvFile = "dt01.csv"
    val dataFile = TestDirs.resolveTestResourcePath("data/e2e/" + csvFile)
    val joinResultFile = "c2p_id_txf"
    val outputPath = TestDirs.resolveOutputPath("parquet")
    val outputType = "parquet"
    val idField = "t_id"
    val finalOutputPath = transform(dataFile,
      toFolder = outputPath, toFileName = joinResultFile, toType = outputType,
      idField = idField, filteredFields = Nil, overwriteOnExist = true)
    assertResult(outputPath + "/c2p_id_txf.parquet")(finalOutputPath)
    val outRdd = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(finalOutputPath))
      .load(finalOutputPath, realIDField = idField, keepRealID = true)
    assertResult(12)(outRdd.count())
    assertResult(
      Profile(0, mutable.MutableList(
        KeyValue("t_id", "TCN001277"), KeyValue("t_amount", "5020000"), KeyValue("t_user", "ll01"),
        KeyValue("t_date", "20200719"), KeyValue("t_pid", "P1007"), KeyValue("t_cptyid", "LC1291003001"),
        KeyValue("site", "CN"), KeyValue("system_id", "ALIBB")), "TCN001277", 0)
    )(outRdd.first())
  }
  it should "transform csv to parquet partially" in {
    val csvFile = "dt01.csv"
    val dataFile = TestDirs.resolveTestResourcePath("data/e2e/" + csvFile)
    val joinResultFile = "dt_filtered_txf"
    val outputPath = TestDirs.resolveOutputPath("parquet")
    val outputType = "parquet"
    val idField = "t_id"
    val fields = List("t_id", "t_amount")
    val finalOutputPath = transform(dataFile,
      toFolder = outputPath, toFileName = joinResultFile, toType = outputType,
      idField = idField, filteredFields = fields, overwriteOnExist = true)
    assertResult(outputPath + "/dt_filtered_txf.parquet")(finalOutputPath)
    val outRdd = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(finalOutputPath))
      .load(finalOutputPath, realIDField = idField, keepRealID = true)
    assertResult(12)(outRdd.count())
    val first = outRdd.first()
    log.info("firstOutput=" + first)
    assertResult(
      Profile(0, mutable.MutableList(
        KeyValue("t_id", "TCN001277"),
        KeyValue("t_amount", "5020000"))
        , "TCN001277", 0)
    )(first)
  }
  it should "transform parquet1 to csv" in {
    val csvFile = "dt01.parquet"
    val dataFile = TestDirs.resolveTestResourcePath("data/e2e/" + csvFile)
    val joinResultFile = "p2c_txf"
    val outputPath = TestDirs.resolveOutputPath("csv")
    val outputType = "csv"
    val idField = "t_id"
    val finalOutputPath = transform(dataFile,
      toFolder = outputPath, toFileName = joinResultFile, toType = outputType,
      idField = idField, filteredFields = Nil, overwriteOnExist = true)
    assertResult(outputPath + "/p2c_txf.csv")(finalOutputPath)
    val outRdd = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(finalOutputPath))
      .load(finalOutputPath, realIDField = idField, keepRealID = true)
    assertResult(12)(outRdd.count())
    val first = outRdd.first()
    log.info("firstOutput=" + first)
    assertResult(
      Profile(0, mutable.MutableList(
        KeyValue("t_id", "TCN001277")), "TCN001277", 0)
    )(first)
  }
  it should "transform parquet2 to csv" in {
    val csvFile = "dt01_c2p_txf.parquet"
    val dataFile = TestDirs.resolveTestResourcePath("data/e2e/" + csvFile)
    val joinResultFile = "p2c2_txf"
    val outputPath = TestDirs.resolveOutputPath("csv")
    val outputType = "csv"
    val idField = "t_id"
    val finalOutputPath = transform(dataFile,
      toFolder = outputPath, toFileName = joinResultFile, toType = outputType,
      idField = idField, filteredFields = Nil, overwriteOnExist = true)
    assertResult(outputPath + "/p2c2_txf.csv")(finalOutputPath)
    val outRdd = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(finalOutputPath))
      .load(finalOutputPath, realIDField = idField, keepRealID = true)
    assertResult(12)(outRdd.count())
    val first = outRdd.first()
    log.info("firstOutput=" + first)
    assertResult(
      Profile(0, mutable.MutableList(
        KeyValue("t_id", "TCN001277"), KeyValue("t_amount", "5020000"), KeyValue("t_user", "ll01"),
        KeyValue("t_date", "20200719"), KeyValue("t_pid", "P1007"), KeyValue("t_cptyid", "LC1291003001"),
        KeyValue("site", "CN"), KeyValue("system_id", "ALIBB")), "TCN001277", 0)
    )(first)
  }
}