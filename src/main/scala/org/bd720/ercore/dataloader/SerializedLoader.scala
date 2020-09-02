package org.bd720.ercore.dataloader
import java.io.{FileInputStream, ObjectInputStream}
import java.util
import org.bd720.ercore.model.{EntityProfile, IdDuplicates}
import scala.io.Source
object SerializedLoader extends scala.AnyRef {
  def loadSerializedGroundtruth(fileName: scala.Predef.String): java.util.HashSet[IdDuplicates] = {
    val in = new ObjectInputStream(new FileInputStream(fileName))
    in.readObject().asInstanceOf[java.util.HashSet[IdDuplicates]]
  }
  def loadSerializedDataset(fileName: scala.Predef.String): java.util.ArrayList[EntityProfile] = {
    val in = new ObjectInputStream(new FileInputStream(fileName))
    in.readObject().asInstanceOf[java.util.ArrayList[EntityProfile]]
  }
}
