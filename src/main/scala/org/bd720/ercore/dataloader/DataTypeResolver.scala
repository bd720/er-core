package org.wumiguo.ser.dataloader
import org.wumiguo.ser.dataloader.DataType.DataType
object DataTypeResolver {
  def getDataType(dataFile: String): DataType = {
    import DataType._
    val theDataFile = dataFile.toLowerCase()
    if (theDataFile.endsWith(".csv")) {
      CSV
    } else if (theDataFile.endsWith(".json")) {
      JSON
    } else if (theDataFile.endsWith(".parquet")) {
      PARQUET
    } else throw new RuntimeException("Out of support data type " + DataType.values)
  }
}
