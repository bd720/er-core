package org.bd720.ercore.flow
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.bd720.ercore.ERFlowLauncher.getClass
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.dataloader.{CSVProfileLoader, GroundTruthLoader}
import org.bd720.ercore.flow.End2EndFlow.log
import org.bd720.ercore.methods.blockbuilding.TokenBlocking
import org.bd720.ercore.methods.blockrefinement.{BlockFiltering, BlockPurging}
import org.bd720.ercore.methods.datastructure.{KeysCluster, Profile}
import org.bd720.ercore.methods.entityclustering.EntityClusterUtils
import org.bd720.ercore.methods.entitymatching.{EntityMatching, MatchingFunctions}
import org.bd720.ercore.methods.util.Converters
object End2EndFlow extends ERFlow with SparkEnvSetup {
  override def run(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    log.info("launch full end2end flow")
    val gtPath = getClass.getClassLoader.getResource("sampledata/dblpAcmIdDuplicates.gen.csv").getPath
    log.info("load ground-truth from path {}", gtPath)
    val gtRdd = GroundTruthLoader.loadGroundTruth(gtPath)
    log.info("gt size is {}", gtRdd.count())
    val ep1Path = getClass.getClassLoader.getResource("sampledata/acmProfiles.gen.csv").getPath
    val ep1Rdd = CSVProfileLoader.loadProfilesAdvanceMode(ep1Path, startIDFrom = 0, separator = ",", header = true, sourceId = 1001)
    log.info("ep1 size is {}", ep1Rdd.count())
    val ep2Path = getClass.getClassLoader.getResource("sampledata/dblpProfiles.gen.csv").getPath
    val ep2Rdd = CSVProfileLoader.loadProfilesAdvanceMode(ep2Path, startIDFrom = 0, separator = ",", header = true, sourceId = 2002)
    log.info("ep2 size is {}", ep2Rdd.count())
    val separators = Array[Int]()
    var clusters = List[KeysCluster]()
    clusters = KeysCluster(100111, List("1001_year", "2002_year")) :: clusters
    clusters = KeysCluster(100112, List("1001_title", "2002_title")) :: clusters
    clusters = KeysCluster(100113, List("1001_authors", "2002_authors")) :: clusters
    clusters = KeysCluster(100114, List("1001_venue", "2002_venue")) :: clusters
    val ep1Blocks = TokenBlocking.createBlocksCluster(ep1Rdd, separators, clusters)
    val ep2Blocks = TokenBlocking.createBlocksCluster(ep2Rdd, separators, clusters)
    log.info("ep1blocks {}", ep1Blocks.count())
    log.info("ep2blocks {}", ep2Blocks.count())
    ep1Blocks.top(10).foreach(b => log.info("ep1b is {}", b))
    ep2Blocks.top(10).foreach(b => log.info("ep2b is {}", b))
    val profileBlocks1 = Converters.blocksToProfileBlocks(ep1Blocks)
    val profileBlocks2 = Converters.blocksToProfileBlocks(ep2Blocks)
    log.info("pb count1 {}", profileBlocks1.count())
    log.info("pb count2 {}", profileBlocks2.count())
    log.info("pb first1 {}", profileBlocks1.first())
    log.info("pb first2 {}", profileBlocks2.first())
    val profileBlockFilter1 = BlockFiltering.blockFiltering(profileBlocks1, ratio = 0.5)
    val profileBlockFilter2 = BlockFiltering.blockFiltering(profileBlocks2, ratio = 0.5)
    log.info("pb count1 {}", profileBlockFilter1.count())
    log.info("pb count2 {}", profileBlockFilter2.count())
    log.info("pb first1 {}", profileBlockFilter1.first())
    log.info("pb first2 {}", profileBlockFilter2.first())
    val abRdd1 = Converters.profilesBlockToBlocks(profileBlockFilter1)
    val abRdd2 = Converters.profilesBlockToBlocks(profileBlockFilter2)
    val pAbRdd1 = BlockPurging.blockPurging(abRdd1, 0.6)
    val pAbRdd2 = BlockPurging.blockPurging(abRdd2, 0.6)
    val broadcastVar = spark.sparkContext.broadcast(ep1Rdd.collect())
    val combinedRdd = ep2Rdd.flatMap(p2 => broadcastVar.value.map(p1 => (p1, p2)))
    combinedRdd.take(3).foreach(x => log.info("combined {}", x))
    val weRdd = combinedRdd.map(x => EntityMatching.profileMatching(x._1, x._2, MatchingFunctions.jaccardSimilarity))
    val connected = EntityClusterUtils.connectedComponents(weRdd)
    connected.top(10).foreach(x => log.info("connected=" + x))
    spark.close()
  }
}
