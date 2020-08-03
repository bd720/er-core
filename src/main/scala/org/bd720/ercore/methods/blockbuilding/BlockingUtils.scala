package org.wumiguo.ser.methods.blockbuilding
object BlockingUtils {
  object TokenizerPattern {
    val DEFAULT_SPLITTING = "[\\W_]"
  }
  def associateKeysToProfileID(profileEntryKeys: (Int, Iterable[String])): Iterable[(String, Int)] = {
    val profileId = profileEntryKeys._1
    val keys = profileEntryKeys._2
    keys.map(key => (key, profileId))
  }
  def associateKeysToProfileIdEntropy(profileEntryKeys: (Int, Iterable[String])): Iterable[(String, (Int, Iterable[Int]))] = {
    val profileId = profileEntryKeys._1
    val tokens = profileEntryKeys._2
    tokens.map(tokens => (tokens, (profileId, tokens.map(_.hashCode))))
  }
}
