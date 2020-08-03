package org.wumiguo.ser.flow
trait ERFlow extends Serializable {
  def run(args: Array[String]): Unit
}
