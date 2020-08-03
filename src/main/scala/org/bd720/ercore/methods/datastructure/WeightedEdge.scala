package org.wumiguo.ser.methods.datastructure
case class WeightedEdge(firstProfileID: Int, secondProfileID: Int, weight: Double) extends EdgeTrait with Serializable {
}
