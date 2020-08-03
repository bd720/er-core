package org.bd720.ercore.methods.blockbuilding
import java.util
import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.jgrapht.alg.ConnectivityInspector
import org.jgrapht.graph.{DefaultEdge, SimpleGraph}
import org.bd720.ercore.methods.datastructure.{BlockAbstract, BlockClean, BlockDirty, KeysCluster, Profile}
import scala.util.Random
object LSH {
  object Settings {
    val DEFAULT_CLUSTER_NAME = "tuttiTokenNonNeiCluster"
    val INITIAL_MSG: Double = -1.0
    val SOURCE_NAME_SEPARATOR = "_"
  }
  def getHashes2(strHash: Int,
                 numHashes: Int,
                 seed: Int = 1234): Array[Int] = {
    val rnd = new Random(seed)
    val hashes = for (i <- 0 until numHashes) yield {
      val a: Int = 1 + rnd.nextInt()
      val b: Int = rnd.nextInt()
      (((a.toLong * strHash.toLong + b.toLong) % 2147495899L) % Integer.MAX_VALUE).toInt
    }
    hashes.toArray
  }
  def getNumBands(targetThreshold: Double, sigNum: Int): Int = {
    var b = sigNum
    def r = sigNum.toDouble / b
    def t = Math.pow(1.0 / b, 1.0 / r)
    while (t < targetThreshold && b > 1) {
      b -= 1
    }
    b + 1
  }
  def getNumRows(targetThreshold: Double, sigNum: Int): Int = {
    val bands = getNumBands(targetThreshold, sigNum)
    val nrows = sigNum / bands
    if (nrows < 1) {
      1
    }
    else {
      nrows
    }
  }
  def calcSimilarity(sig1: Array[Int], sig2: Array[Int]): Double = {
    var common: Double = 0
    for (i <- sig1.indices if sig1(i) == sig2(i)) {
      common += 1
    }
    common / sig1.length.toDouble
  }
  case class Attr(sourceName: Int, attribute: String)
  def clusterSimilarAttributes(profiles: RDD[Profile], numHashes: Int, targetThreshold: Double, maxFactor: Double, numBands: Int = -1, keysToExclude: Iterable[String] = Nil, computeEntropy: Boolean = false, separator: String = Settings.SOURCE_NAME_SEPARATOR): List[KeysCluster] = {
    @transient lazy val log = org.apache.log4j.LogManager.getRootLogger
    val t0 = Calendar.getInstance()
    val attributesToken: RDD[(Attr, String)] = profiles.flatMap {
      profile =>
        val attributes = profile.attributes.filter(kv => !keysToExclude.exists(_.equals(kv.key)))
        attributes.flatMap {
          kv =>
            kv.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).filter(_.trim.nonEmpty).map(_.toLowerCase).map(token => (Attr(profile.sourceId, kv.key), token))
        }
    }
    val tokenAttribute_to_tokenID = attributesToken.map(_.swap).groupByKey().zipWithIndex()
    val attributesPerToken: RDD[(Int, Iterable[Attr])] = tokenAttribute_to_tokenID.map { case ((token, attribueNameList), tokenID) => (tokenID.toInt, attribueNameList) }
    val sc = SparkContext.getOrCreate()
    val hashes2 = sc.broadcast(attributesPerToken.map { case (tokenID, dataprofiles) =>
      val hashes = getHashes2(tokenID, numHashes)
      (tokenID, hashes)
    }.collectAsMap())
    val tokensPerAttribute: RDD[(Attr, Set[Int])] = attributesPerToken.flatMap { case (tokenID, attributes) =>
      attributes.map(p => (p, tokenID))
    }.groupByKey().map(x => (x._1, x._2.toSet))
    val allAttributes = tokensPerAttribute.map(_._1)
    val attributeWithSignature: RDD[(Attr, Array[Int])] = tokensPerAttribute.map { case (attribute, tokens) =>
      val signature = Array.fill[Int](numHashes) {
        Int.MaxValue
      }
      tokens.foreach { t =>
        val h = hashes2.value(t)
        for (i <- h.indices) {
          if (h(i) < signature(i)) {
            signature.update(i, h(i))
          }
        }
      }
      (attribute, signature)
    }
    log.info("SPARKER - Num bands " + getNumBands(targetThreshold, numHashes))
    val numRows = getNumRows(targetThreshold, numHashes)
    println("NUMERO DI RIGHE " + numRows)
    val buckets = attributeWithSignature.map { case (attribute, signature) =>
      val bands = signature.sliding(numRows, numRows)
      val bucketIDs = bands.map(_.toList.hashCode()).toIterable
      (attribute, bucketIDs)
    }
    val attributesPerBucket = buckets
      .flatMap { case (attribute, bucketIDs) => bucketIDs.map((_, attribute)) }.groupByKey()
      .map { case (bucketID, attributeList) => (bucketID, attributeList.toSet) }
      .filter { x => x._2.size > 1 && x._2.size < 101 }
      .map(_._2).distinct()
    val attributeSignatures = attributeWithSignature.collectAsMap()
    hashes2.unpersist()
    val t1 = Calendar.getInstance()
    log.info("SPARKER - Time to perform LSH " + (t1.getTimeInMillis - t0.getTimeInMillis) + " ms")
    val numbuckets = attributesPerBucket.count()
    val t2 = Calendar.getInstance()
    log.info("SPARKER - Number of buckets " + numbuckets)
    log.info("SPARKER - Time to calculate attributesPerBucket " + (t2.getTimeInMillis - t1.getTimeInMillis) + " ms")
    val partialClusters = attributesPerBucket
    val attributeSignaturesBroadcast = sc.broadcast(attributeSignatures)
    val edges = partialClusters.flatMap { clusterElements =>
      clusterElements.toList.combinations(2).filter { x =>
        x.head.sourceName != x.last.sourceName
      }.map(x => (x.head, (x.last, calcSimilarity(attributeSignaturesBroadcast.value(x.head), attributeSignaturesBroadcast.value(x.last)))))
    }
    edges.count()
    val t3 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate edges " + (t3.getTimeInMillis - t2.getTimeInMillis) + " ms")
    val edgesPerKey =
      edges.union(
        edges.map { case (attr1, (attr2, sim)) =>
          (attr2, (attr1, sim))
        }
      ).groupByKey().map(x => (x._1, x._2.toSet))
    val topEdges = edgesPerKey.map { case (key1, keys2) =>
      val max = keys2.map(_._2).max * maxFactor
      (key1, keys2.filter(_._2 >= max).map(_._1))
    }
    topEdges.count()
    val t4 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate top edges " + (t4.getTimeInMillis - t3.getTimeInMillis) + " ms")
    val graph = new SimpleGraph[Attr, DefaultEdge](classOf[DefaultEdge])
    val vertices = topEdges.map(_._1).union(topEdges.flatMap(_._2)).distinct().collect()
    vertices.foreach { v =>
      graph.addVertex(v)
    }
    topEdges.collect().foreach { case (from, to) =>
      to.foreach { n =>
        graph.addEdge(from, n)
      }
    }
    attributeSignaturesBroadcast.unpersist()
    val ci = new ConnectivityInspector(graph)
    val connectedComponents = ci.connectedSets()
    val clusters: Iterable[(Iterable[Attr], Int)] = (for (i <- 0 until connectedComponents.size()) yield {
      val a = connectedComponents.get(i).asInstanceOf[util.HashSet[Attr]].iterator()
      var l: List[Attr] = Nil
      while (a.hasNext) {
        l = a.next() :: l
      }
      (l, i)
    }).filter(_._1.nonEmpty)
    val t5 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate clusters " + (t5.getTimeInMillis - t4.getTimeInMillis) + " ms")
    attributeSignaturesBroadcast.destroy()
    val defaultClusterID = {
      if (clusters.isEmpty) {
        0
      }
      else {
        clusters.map(_._2).max + 1
      }
    }
    val clusteredAttributes = clusters.flatMap(_._1).toSet
    val nonClusteredAttributes = allAttributes.collect().filter(!clusteredAttributes.contains(_))
    if (computeEntropy) {
      val keyClusterMap = clusters.flatMap {
        case (attributes, clusterID) =>
          attributes.map(attribute => (attribute, clusterID))
      }.toMap
      val normalizeEntropy = false
      val entropyPerAttribute = attributesToken.groupByKey().map {
        case (attribute, tokens) =>
          val numberOfTokens = tokens.size.toDouble
          val tokensCount = tokens.groupBy(x => x).map(x => x._2.size)
          val tokensP = tokensCount.map {
            tokenCount =>
              val p_i: Double = tokenCount / numberOfTokens
              p_i * (Math.log10(p_i) / Math.log10(2.0d))
          }
          val entropy = {
            if (normalizeEntropy) {
              -tokensP.sum / (Math.log10(numberOfTokens) / Math.log10(2.0d))
            }
            else {
              -tokensP.sum
            }
          }
          (attribute, entropy)
      }
      attributesToken.unpersist()
      log.info(entropyPerAttribute.collect().toLis
      val entropyPerCluster = entropyPerAttribute.map {
        case (attribute, entropy) =>
          val clusterID = keyClusterMap.get(attribute) //Obain the cluster ID
          if (clusterID.isDefined) {
            (clusterID.get, entropy)
          }
          else {
            (defaultClusterID, entropy)
          }
      }.groupByKey().map(x => (x._1, x._2.sum / x._2.size))
      val entropyMap = entropyPerCluster.collectAsMap()
      val defaultEntropy = {
        val e = entropyMap.get(defaultClusterID)
        if (e.isDefined) {
          e.get
        }
        else {
          0.0
        }
      }
      /* Compose everything together */
      clusters.map {
        case (keys, clusterID) =>
          val entropy = {
            val e = entropyMap.get(clusterID)
            if (e.isDefined) {
              e.get
            }
            else {
              1
            }
          }
          KeysCluster(clusterID, keys.map(k => k.sourceName + separator + k.attribute).toList, entropy)
      }.toList ::: KeysCluster(defaultClusterID, nonClusteredAttributes.map(k => k.sourceName + separator + k.attribute).toList, defaultEntropy) :: Nil
    }
    else {
      clusters.map {
        case (keys, clusterID) =>
          KeysCluster(clusterID, keys.map(k => k.sourceName + separator + k.attribute).toList)
      }.toList ::: KeysCluster(defaultClusterID, nonClusteredAttributes.map(k => k.sourceName + separator + k.attribute).toList) :: Nil
    }
  }
  def createBlocks(profiles: RDD[Profile],
                   numHashes: Int,
                   targetThreshold: Double,
                   numBands: Int = -1,
                   separatorIDs: Array[Int] = Array.emptyIntArray, keysToExclude: Iterable[String] = Nil): RDD[BlockAbstract] = {
    @transient lazy val log = org.apache.log4j.LogManager.getRootLogger
    val t0 = Calendar.getInstance()
    /* Generate the tokens */
    val profilesToken: RDD[(Int, String)] = profiles.flatMap {
      profile =>
        val attributes = profile.attributes.filter(kv => !keysToExclude.exists(_.equals(kv.key)))
        attributes.flatMap {
          kv =>
            kv.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).filter(_.trim.nonEmpty).map(_.toLowerCase).map(x => (profile.id, x))
        }
    }
    val profilesPerToken = profilesToken.map(_.swap).groupByKey().zipWithIndex().map(x => (x._2.toInt, x._1._2))
    val sc = SparkContext.getOrCreate()
    val hashes2 = sc.broadcast(profilesPerToken.map { case (tokenID, profiles2) =>
      val hashes = getHashes2(tokenID, numHashes)
      (tokenID, hashes)
    }.collectAsMap())
    val tokensPerAttribute = profilesPerToken.flatMap { case (tokenID, profiles2) =>
      profiles2.map(p => (p, tokenID))
    }.groupByKey().map(x => (x._1, x._2.toSet))
    val profilesWithSignature: RDD[(Int, Array[Int])] = tokensPerAttribute.map { case (profileID, tokens) =>
      val signature = Array.fill[Int](numHashes) {
        Int.MaxValue
      }
      tokens.foreach { t =>
        val h = hashes2.value(t)
        for (i <- h.indices) {
          if (h(i) < signature(i)) {
            signature.update(i, h(i))
          }
        }
      }
      (profileID, signature)
    }
    log.info("SPARKER - Num bands " + getNumBands(targetThreshold, numHashes))
    val numRows = getNumRows(targetThreshold, numHashes)
    val buckets = profilesWithSignature.map { case (attribute, signature) =>
      val buckets = signature.sliding(numRows, numRows).map(_.toList.hashCode()).toIterable
      (attribute, buckets)
    }
    val profilesPerBucket = buckets.flatMap(x => x._2.map((_, x._1))).groupByKey().map(x => (x._1, x._2.toSet)).filter(x => x._2.size > 1).distinct()
    /* Transform each bucket in blocks */
    profilesPerBucket.map {
      case (bucketID, profileIDs) =>
        if (separatorIDs.isEmpty) {
          BlockDirty(bucketID, Array(profileIDs))
        }
        else {
          BlockClean(bucketID, TokenBlocking.separateProfiles(profileIDs, separatorIDs))
        }
    }.filter(_.getComparisonSize() > 0).map(x => x)
  }
}