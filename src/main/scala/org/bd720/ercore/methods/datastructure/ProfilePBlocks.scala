package org.wumiguo.ser.methods.datastructure
case class ProfilePBlocks(profileID : Long, profile : Profile, blocks : Set[BlockWithComparisonSize]) extends Serializable{}
