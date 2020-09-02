package org.bd720.ercore.methods.datastructure
case class ProfilePBlocks(profileID : Long, profile : Profile, blocks : Set[BlockWithComparisonSize]) extends Serializable{}
