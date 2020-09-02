package org.bd720.ercore.flow
trait ERFlow extends Serializable {
  def run(args: Array[String]): Unit
}
