package org.wumiguo.ser.methods.datastructure
case class ProfileBlocks(profileID: Int, blocks: Set[BlockWithComparisonSize]) extends Serializable {}
