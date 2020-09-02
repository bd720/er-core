package org.bd720.ercore.datatransformer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory
import org.bd720.ercore.dataloader.{DataTypeResolver, ProfileLoaderFactory}
import org.bd720.ercore.dataloader.filter.SpecificFieldValueFilter
import org.bd720.ercore.datawriter.GenericDataWriter
import org.bd720.ercore.datawriter.GenericDataWriter.generateOutputWithSchema
import org.bd720.ercore.methods.datastructure.Profile
object GenericDataTransformer {
  val log = LoggerFactory.getLogger(getClass.getName)
  def transform(fromFile: String, toFolder: String, toFileName: String,
                toType: String = "parquet", idField: String = "", filteredFields: List[String] = Nil,
                overwriteOnExist: Boolean = false): String = {
    log.info("load data from file {}", fromFile)
    val resolvedFilteredFields = filteredFields.filter(!_.equals(idField))
    val keepRealID = idField != null && !idField.isEmpty
    val profiles: RDD[Profile] = load(fromFile, idField, filteredFields, keepReadID = keepRealID)
    val first = profiles.first()
    log.info("first-profiles={}", first)
    var columnNames = Seq[String]()
    if (keepRealID) {
      columnNames :+= idField
    }
    columnNames ++= {
      if (resolvedFilteredFields == Nil) {
        first.attributes.filter(_.key != idField).map(_.key).toList
      } else {
        resolvedFilteredFields
      }
    }
    val rows = profiles.map(x => Row.fromSeq(x.attributes.map(y => y.value)))
    val finalPath = generateOutputWithSchema(columnNames, rows,
      toFolder, toType, toFileName, overwriteOnExist)
    log.info("transformed as " + toType + " to path " + finalPath)
    finalPath
  }
  private def load(fromFile: String, idField: String, resolvedFilteredFields: List[String], keepReadID: Boolean): RDD[Profile] = {
    val loader = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(fromFile))
    val profiles = loader.load(fromFile, realIDField = idField,
      startIDFrom = 0, sourceId = 0, keepRealID = keepReadID,
      fieldsToKeep = resolvedFilteredFields,
      fieldValuesScope = Nil,
      filter = SpecificFieldValueFilter
    )
    if (profiles.isEmpty()) {
      throw new RuntimeException("No profile from input " + fromFile)
    }
    profiles
  }
}
