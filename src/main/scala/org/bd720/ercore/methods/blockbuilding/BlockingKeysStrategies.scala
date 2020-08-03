package org.bd720.ercore.methods.blockbuilding
import org.bd720.ercore.methods.datastructure.KeyValue
object BlockingKeysStrategies {
  val minSuffixLen = 2
  val ngramSize = 3
  val max_q_grams = 15
  val extended_qgrams_threshold = 0.95
  def createKeysFromProfileAttributes(attributes: Iterable[KeyValue], keysToExclude: Iterable[String] = Nil): Iterable[String] = {
    attributes.map {
      at =>
        if (keysToExclude.exists(_.equals(at.key))) {
          ""
        }
        else {
          at.value.toLowerCase
        }
    } filter (_.trim.length > 0) flatMap {
      value =>
        value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)
    }
  }
  def createNgramsFromProfileAttributes(attributes: Iterable[KeyValue], keysToExclude: Iterable[String] = Nil): Iterable[String] = {
    val tokens = attributes.flatMap {
      at =>
        if (keysToExclude.exists(_.equals(at.key))) {
          Nil
        }
        else {
          at.value.toLowerCase.trim.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)
        }
    }.filter(_.trim.length > 0)
    val ngrams = tokens.flatMap {
      value =>
        value.sliding(ngramSize)
    }
    ngrams.filter(_.trim.length > 0)
    /*attributes.map {
      at =>
        if (keysToExclude.exists(_.equals(at.key))) {
          ""
        }
        else {
          at.value.toLowerCase.trim.replace(" ", "_")
        }
    } filter (_.trim.length > 0) flatMap {
      value =>
        value.sliding(ngramSize)
    }*/
  }
  def createSuffixesFromProfileAttributes(attributes: Iterable[KeyValue], keysToExclude: Iterable[String] = Nil): Iterable[String] = {
    val tokens = createKeysFromProfileAttributes(attributes, keysToExclude)
    tokens.flatMap(blockingKey => getSuffixes(minSuffixLen, blockingKey))
  }
  def createExtendedSuffixesFromProfileAttributes(attributes: Iterable[KeyValue], keysToExclude: Iterable[String] = Nil): Iterable[String] = {
    val tokens = createKeysFromProfileAttributes(attributes, keysToExclude)
    tokens.flatMap(blockingKey => getExtendedSuffixes(minSuffixLen, blockingKey))
  }
  def createExtendedQgramsFromProfileAttributes(attributes: Iterable[KeyValue], keysToExclude: Iterable[String] = Nil): Iterable[String] = {
    val tokens = attributes.flatMap {
      at =>
        if (keysToExclude.exists(_.equals(at.key))) {
          Nil
        }
        else {
          at.value.toLowerCase.trim.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)
        }
    }.filter(_.trim.length > 0)
    val ngrams = tokens.map {
      value =>
        value.sliding(ngramSize).toList
    }.toArray
    var res: Set[String] = Set.empty[String]
    ngrams.foreach { n =>
      if (n.length == 1) {
        res = res + n.head
      }
      else {
        val n1 = n.take(max_q_grams)
        val minLen = math.max(1, math.floor(n.length * extended_qgrams_threshold))
        for (i <- minLen.toInt to n.length) {
          res = res ++ getCombinationsFor(n, i)
        }
      }
    }
    res
  }
  def getCombinationsFor(sublists: List[String], subListsLen: Int): Set[String] = {
    if (subListsLen == 0 || sublists.size < subListsLen) {
      Set.empty[String]
    }
    else {
      val remainingElements = sublists.take(sublists.size - 1)
      val lastSublist = sublists.last
      val combinationsExclusiveX = getCombinationsFor(remainingElements, subListsLen)
      val combinationsInclusiveX = getCombinationsFor(remainingElements, subListsLen - 1)
      var res = combinationsExclusiveX
      if (combinationsInclusiveX.isEmpty) {
        res = res + lastSublist
      }
      else {
        combinationsInclusiveX.foreach { comb =>
          res = res + (comb + lastSublist)
        }
      }
      res
    }
  }
  /*
  def getSuffixes(minimumLength: Int, blockingKey: String): Set[String] = {
    var suffixes: List[String] = Nil
    if (blockingKey.length < minimumLength) {
      suffixes = blockingKey :: suffixes
    }
    else {
      val limit: Int = blockingKey.length - minimumLength + 1
      for (i <- 0 until limit) {
        suffixes = blockingKey.substring(i:: suffixes
      }
    }
    suffixes.toSet
  }
  def getExtendedSuffixes(minimumLength: Int, blockingKey: String): Set[String] = {
    var suffixes: List[String] = List(blockingKey)
    if (minimumLength <= blockingKey.length()) {
      for (nGramSize <- blockingKey.length() - 1 to minimumLength by -1) {
        var currentPosition = 0
        val length = blockingKey.length() - (nGramSize - 1)
        while (currentPosition < length) {
          suffixes = blockingKey.substring(currentPosition, currentPosition + nGramSize) :: suffixes
          currentPosition += 1
        }
      }
    }
    suffixes.toSet
  }
  def createNgramsFromProfileAttributes2(attributes: Iterable[KeyValue], keysToExclude: Iterable[String] = Nil): Iterable[String] = {
    attributes.map {
      at =>
        if (keysToExclude.exists(_.equals(at.key))) {
          ("", "")
        }
        else {
          (at.key, at.value.toLowerCase.trim.replace(" ", "_"))
        }
    }.filter(x => x._2.trim.length > 0).flatMap { case (key, value) =>
      value.sliding(3).map(v => key + "_" + v)
    }
  }
}
