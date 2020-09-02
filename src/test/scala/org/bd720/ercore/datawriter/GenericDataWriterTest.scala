package org.bd720.ercore.datawriter
import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.dataloader.{CSVProfileLoader, DataTypeResolver, ParquetProfileLoader, ProfileLoaderFactory}
import org.bd720.ercore.methods.datastructure.{KeyValue}
import org.bd720.ercore.testutil.TestDirs
import scala.collection.mutable
class GenericDataWriterTest extends AnyFlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(this.getClass.getName)
  it should "write parquet " in {
    val overwriteOnExist = true
    val joinResultFile = "res"
    val outputPath = TestDirs.resolveOutputPath("parquet")
    val outputType = "parquet"
    val realIDField = "u_id"
    val columnNames = List(realIDField, "username")
    val rawRows = Seq(
      Row.fromTuple(("LUSLD9921", "lev")),
      Row.fromTuple(("DKJKSE021", "vin")),
      Row.fromTuple(("UEJLS9291", "joh")),
      Row.fromTuple(("YYYEKE021", "ohn"))
    )
    val rows = spark.sparkContext.makeRDD(rawRows)
    val finalOutputPath = GenericDataWriter.generateOutputWithSchema(columnNames, rows,
      outputPath, outputType, joinResultFile, overwriteOnExist)
    log.info("outputPath=" + finalOutputPath)
    assertResult(outputPath + "/res.parquet")(finalOutputPath)
    val loader = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(finalOutputPath))
    assert(loader.isInstanceOf[ParquetProfileLoader.type])
    assert(!loader.isInstanceOf[CSVProfileLoader.type])
    val data = loader.load(finalOutputPath, realIDField = realIDField,keepRealID = true)
    val outKv = data.map(p => (p.getAttributeValues(realIDField), p.attributes.toList)).collect().toList
      .map(x => Map(x._1 -> x._2)).reduce(_ ++ _)
    val rawKv = rawRows.map(x => mutable.Map(x.getString(columnNames.indexOf(realIDField)) -> columnNames.map(name => KeyValue(name, x.getString(columnNames.indexOf(name))))))
      .reduce(_ ++ _)
    assertResult(4)(outKv.size)
    assertResult(rawKv)(outKv)
  }
}
