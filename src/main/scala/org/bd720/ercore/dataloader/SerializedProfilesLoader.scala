package org.wumiguo.ser.dataloader
import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.wumiguo.ser.methods.datastructure.Profile
object SerializedProfilesLoader {
  def loadProfiles(filePath: String, chunkSize: Int = 10000, startIDFrom: Int = -1, sourceId: Int = 0): RDD[Profile] = {
    val sc = SparkContext.getOrCreate()
    val data = loadSerializedObject(filePath).asInstanceOf[Array[Profile]]
    val profiles = sc.union(data.grouped(chunkSize).map(sc.parallelize(_)).toArray)
    if (startIDFrom > 0) {
      profiles.map(p => Profile(p.id + startIDFrom, p.attributes, p.originalID, sourceId))
    }
    else {
      profiles
    }
  }
  def loadSerializedObject(fileName: String): Any = {
    var `object`: Any = null
    try {
      val file: InputStream = new FileInputStream(fileName)
      val buffer: InputStream = new BufferedInputStream(file)
      val input: ObjectInput = new ObjectInputStream(buffer)
      try {
        `object` = input.readObject
      } finally {
        input.close()
      }
    }
    catch {
      case cnfEx: ClassNotFoundException => {
        System.err.println(fileName)
        cnfEx.printStackTrace()
      }
      case ioex: IOException => {
        System.err.println(fileName)
        ioex.printStackTrace()
      }
    }
    return `object`
  }
}
