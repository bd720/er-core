package org.wumiguo.ser.methods.blockrefinement
import org.apache.spark.rdd.RDD
import org.wumiguo.ser.methods.datastructure.BlockAbstract
object BlockPurging {
  def blockPurging(blocks : RDD[BlockAbstract], smoothFactor : Double) : RDD[BlockAbstract] = {
    val blocksComparisonsAndSizes = blocks map {
      block => (block.getComparisonSize(), block.size)
    }
    val blockComparisonsAndSizesPerComparisonLevel = blocksComparisonsAndSizes map {
      blockComparisonAndSize => (blockComparisonAndSize._1, blockComparisonAndSize)
    }
    val totalNumberOfComparisonsAndSizePerComparisonLevel = blockComparisonsAndSizesPerComparisonLevel.reduceByKey((x, y) => (x._1+y._1, x._2+y._2))
    val totalNumberOfComparisonsAndSizePerComparisonLevelSorted = totalNumberOfComparisonsAndSizePerComparisonLevel.sortBy(_._1).collect().toList
    val totalNumberOfComparisonsAndSizePerComparisonLevelSortedAdded = sumPrecedentLevels(totalNumberOfComparisonsAndSizePerComparisonLevelSorted)
    val maximumNumberOfComparisonAllowed = calcMaxComparisonNumber(totalNumberOfComparisonsAndSizePerComparisonLevelSortedAdded.toArray, smoothFactor)
    val log = org.apache.log4j.Logger.getRootLogger
    log.info("SPARKER - BLOCK PURGING COMPARISONS MAX "+maximumNumberOfComparisonAllowed)
    blocks filter{
      block =>
        block.getComparisonSize() <= maximumNumberOfComparisonAllowed
    }
  }
  def sumPrecedentLevels(input : Iterable[(Double, (Double, Double))]) : Iterable[(Double, (Double, Double))] = {
    /*val out : ListBuffer[(Double, (Double, Double))] = new ListBuffer[(Double, (Double, Double))]()
    out ++= input
    for(i <- 0 to input.size-2){
      out(i+1) = (out(i+1)._1, (out(i+1)._2._1+out(i)._2._1, out(i+1)._2._2+out(i)._2._2))
    }
    out.toList*/
    if(input.isEmpty){
      input
    }
    else{
      input.tail.scanLeft(input.head)((acc, x) => (x._1, (x._2._1+acc._2._1, x._2._2+acc._2._2)))
    }
  }
  def calcMaxComparisonNumber(input : Array[(Double, (Double, Double))], smoothFactor : Double) : Double = {
    var currentBC : Double = 0
    var currentCC : Double = 0
    var currentSize : Double = 0
    var previousBC : Double = 0
    var previousCC : Double = 0
    var previousSize : Double = 0
    val arraySize = input.length
    for(i <- arraySize-1 to 0 by -1) {
      previousSize = currentSize
      previousBC = currentBC
      previousCC = currentCC
      currentSize = input(i)._1     //Current comparison level
      currentBC = input(i)._2._2    //Current comparison level number of elements
      currentCC = input(i)._2._1    //Current comparison level number of comparisons
      if (currentBC * previousCC < smoothFactor * currentCC * previousBC) {
        return previousSize
      }
    }
    previousSize
  }
}
