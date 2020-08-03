package org.wumiguo.ser.dataloader
import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import org.wumiguo.ser.model.{EntityProfile, IdDuplicates}
object SerializedWriter extends scala.AnyRef {
  def serializedGroundTruth(fileName: scala.Predef.String, data: java.util.HashSet[IdDuplicates]) = serializedObject(fileName, data)
  def serializedEntityProfiles(fileName: scala.Predef.String, data: java.util.ArrayList[EntityProfile]) = serializedObject(fileName, data)
  def serializedObject(fileName: scala.Predef.String, data: Any) = {
    val fos = new FileOutputStream(fileName)
    try {
      val out = new ObjectOutputStream(fos)
      try out.writeObject(data)
      finally out.close()
    }
    finally fos.close()
  }
}
