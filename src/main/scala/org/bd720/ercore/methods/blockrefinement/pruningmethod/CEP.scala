package org.bd720.ercore.methods.blockrefinement.pruningmethod
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.blockrefinement.pruningmethod.PruningUtils.WeightTypes
import org.bd720.ercore.methods.datastructure.{ProfileBlocks, UnweightedEdge}
import scala.collection.mutable
object CEP {
  def CEP(profileBlocksFiltered: RDD[ProfileBlocks],
          blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
          maxID: Int,
          separatorIDs: Array[Int],
          groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]],
          weightType: String = WeightTypes.CBS,
          profileBlocksSizeIndex: Broadcast[scala.collection.Map[Int, Int]] = null,
          useEntropy: Boolean = false,
          blocksEntropies: Broadcast[scala.collection.Map[Int, Double]] = null
         )
  : RDD[(Double, Double, Iterable[UnweightedEdge])] = {
    val edgesToKeep = math.floor(blockIndex.value.values.flatMap(_.map(_.size.toDouble)).sum / 2.0)
    println("SPARKER - Number of edges to keep " + edgesToKeep)
    if (useEntropy && blocksEntropies == null) {
      throw new Exception("blocksEntropies must be defined")
    }
    if ((weightType == WeightTypes.ECBS || weightType == WeightTypes.EJS || weightType == WeightTypes.JS || weightType == WeightTypes.chiSquare) && profileBlocksSizeIndex == null) {
      throw new Exception("profileBlocksSizeIndex must be defined")
    }
    if (!List(WeightTypes.CBS, WeightTypes.JS, WeightTypes.chiSquare, WeightTypes.ARCS, WeightTypes.ECBS, WeightTypes.EJS).contains(weightType)) {
      throw new Exception("Please provide a valid WeightType, " + weightType + " is not an acceptable value!")
    }
    val sc = SparkContext.getOrCreate()
    var numberOfEdges: Double = 0
    var edgesPerProfile: Broadcast[scala.collection.Map[Int, Double]] = null
    if (weightType == WeightTypes.EJS) {
      val stats = CommonNodePruning.computeStatistics(profileBlocksFiltered, blockIndex, maxID, separatorIDs)
      numberOfEdges = stats.map(_._1.toDouble).sum()
      edgesPerProfile = sc.broadcast(stats.map(x => (x._2._1, x._2._2.toDouble)).groupByKey().map(x => (x._1, x._2.sum)).collectAsMap())
    }
    val threshold = calcThreshold(profileBlocksFiltered, blockIndex, maxID, separatorIDs, weightType, profileBlocksSizeIndex, useEntropy, blocksEntropies, numberOfEdges, edgesPerProfile, edgesToKeep)
    println("SPARKER - Computed threshold " + threshold)
    val toKeep = sc.broadcast(threshold._2)
    val edges = pruning(profileBlocksFiltered, blockIndex, maxID, separatorIDs, groundtruth, weightType, profileBlocksSizeIndex, useEntropy, blocksEntropies, threshold._1, toKeep, numberOfEdges, edgesPerProfile)
    toKeep.unpersist()
    if (edgesPerProfile != null) {
      edgesPerProfile.unpersist()
    }
    edges
  }
  def calcFreq(weights: Array[Double],
               neighbors: Array[Int],
               neighborsNumber: Int): Array[(Double, Double)] = {
    var acc: Double = 0
    val out = mutable.Map[Double, Double]()
    for (i <- 0 until neighborsNumber) {
      val weight = weights(neighbors(i))
      val w = out.getOrElse(weight, 0.0) + 1
      out.put(weight, w)
    }
    out.toArray
  }
  def doPruning(profileID: Int,
                weights: Array[Double],
                neighbors: Array[Int],
                neighborsNumber: Int,
                groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]],
                weightType: String,
                globalThreshold: Double,
                toKeep: Broadcast[scala.collection.Map[Int, Double]]
               ): (Double, Double, Iterable[UnweightedEdge]) = {
    var cont: Double = 0
    var gtFound: Double = 0
    var edges: List[UnweightedEdge] = Nil
    var neighborToKeep = toKeep.value.getOrElse(profileID, 0.0).toInt
    for (i <- 0 until neighborsNumber) {
      val neighborID = neighbors(i)
      val neighborWeight = weights(neighborID)
      if ((neighborWeight > globalThreshold) || (neighborWeight == globalThreshold && neighborToKeep > 0)) {
        cont += 1
        if (groundtruth != null && groundtruth.value.contains((profileID, neighborID))) {
          gtFound += 1
        }
        edges = UnweightedEdge(profileID, neighbors(i)) :: edges
        if (neighborWeight == globalThreshold) {
          neighborToKeep -= 1
        }
      }     }
    (cont, gtFound, edges)
  }
  def calcThreshold(profileBlocksFiltered: RDD[ProfileBlocks],
                    blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
                    maxID: Int,
                    separatorID: Array[Int],
                    weightType: String,
                    profileBlocksSizeIndex: Broadcast[scala.collection.Map[Int, Int]],
                    useEntropy: Boolean,
                    blocksEntropies: Broadcast[scala.collection.Map[Int, Double]],
                    numberOfEdges: Double,
                    edgesPerProfile: Broadcast[scala.collection.Map[Int, Double]],
                    edgesToKeep: Double
                   ): (Double, scala.collection.Map[Int, Double]) = {
    val partialFreqs = profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) {
        0
      }
      val neighbors = Array.ofDim[Int](maxID + 1)
      var neighborsNumber = 0
      val entropies: Array[Double] = {
        if (useEntropy) {
          Array.fill[Double](maxID + 1) {
            0.0
          }
        }
        else {
          null
        }
      }
      partition.map { pb =>
        neighborsNumber = CommonNodePruning.calcCBS(pb, blockIndex, separatorID, useEntropy, blocksEntropies, localWeights, entropies, neighbors, false)
        CommonNodePruning.calcWeights(pb, localWeights, neighbors, entropies, neighborsNumber, blockIndex, separatorID, weightType, profileBlocksSizeIndex, useEntropy, numberOfEdges, edgesPerProfile)
        val localFreq = calcFreq(localWeights, neighbors, neighborsNumber)
        CommonNodePruning.doReset(localWeights, neighbors, entropies, useEntropy, neighborsNumber)
        (pb.profileID, localFreq.toList)
      }
    }
    val frequencies = partialFreqs.flatMap(x => x._2).groupByKey().map(x => (x._1, x._2.sum)).collect().sortBy(-_._1)
    var sum = 0.0
    var cont = 0
    while (sum < edgesToKeep && cont < frequencies.length) {
      sum += frequencies(cont)._2
      cont += 1
    }
    val threshold = frequencies(cont - 1)._1
    var lastToKeep = frequencies(cont - 1)._2 - (sum - edgesToKeep)
    println("SPARKER - Di quelli con peso uguale a  " + threshold + " devo tenerne " + lastToKeep)
    val stats = partialFreqs.map { p =>
      (p._1, p._2.filter(_._1 == threshold).map(_._2))
    }.filter(_._2.nonEmpty).map(x => (x._1, x._2.head)).collect()
    val toKeep = for (i <- stats.indices; if lastToKeep > 0) yield {
      if (stats(i)._2 > lastToKeep) {
        val a = lastToKeep
        lastToKeep -= stats(i)._2
        (stats(i)._1, a)
      }
      else {
        lastToKeep -= stats(i)._2
        stats(i)
      }
    }
    (threshold, toKeep.filter(_._2 > 0).toMap)
  }
  def pruning(profileBlocksFiltered: RDD[ProfileBlocks],
              blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
              maxID: Int,
              separatorID: Array[Int],
              groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]],
              weightType: String,
              profileBlocksSizeIndex: Broadcast[scala.collection.Map[Int, Int]],
              useEntropy: Boolean,
              blocksEntropies: Broadcast[scala.collection.Map[Int, Double]],
              threshold: Double,
              toKeep: Broadcast[scala.collection.Map[Int, Double]],
              numberOfEdges: Double,
              edgesPerProfile: Broadcast[scala.collection.Map[Int, Double]]
             ): RDD[(Double, Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) {
        0
      }
      val neighbors = Array.ofDim[Int](maxID + 1)
      var neighborsNumber = 0
      val entropies: Array[Double] = {
        if (useEntropy) {
          Array.fill[Double](maxID + 1) {
            0.0
          }
        }
        else {
          null
        }
      }
      partition.map { pb =>
        neighborsNumber = CommonNodePruning.calcCBS(pb, blockIndex, separatorID, useEntropy, blocksEntropies, localWeights, entropies, neighbors, false)
        CommonNodePruning.calcWeights(pb, localWeights, neighbors, entropies, neighborsNumber, blockIndex, separatorID, weightType, profileBlocksSizeIndex, useEntropy, numberOfEdges, edgesPerProfile)
        val result = doPruning(pb.profileID, localWeights, neighbors, neighborsNumber, groundtruth, weightType, threshold, toKeep)
        CommonNodePruning.doReset(localWeights, neighbors, entropies, useEntropy, neighborsNumber)
        result
      }
    }
  }
}
