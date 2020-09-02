package org.bd720.ercore.dataloader
import org.bd720.ercore.dataloader.DataType.DataType
object ProfileLoaderFactory {
  val DATA_TYPE_CSV = "CSV"
  val DATA_TYPE_JSON = "JSON"
  val DATA_TYPE_PARQUET = "PARQUET"
  def getDataLoader(sourceType: DataType): ProfileLoaderTrait = {
    import DataType._
    sourceType match {
      case CSV => CSVProfileLoader
      case JSON => JSONProfileLoader
      case PARQUET => ParquetProfileLoader
    }
  }
}
