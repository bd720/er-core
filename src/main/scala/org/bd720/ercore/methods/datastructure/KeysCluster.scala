package org.wumiguo.ser.methods.datastructure
case class KeysCluster(id : Int, keys: List[String], entropy : Double = 1, filtering : Double = 0.8){}
