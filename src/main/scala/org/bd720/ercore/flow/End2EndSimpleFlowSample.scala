package org.bd720.ercore.flow
import org.apache.spark.sql.SparkSession
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.dataloader.{CSVProfileLoader, DataTypeResolver, GroundTruthLoader, ProfileLoaderFactory}
import org.bd720.ercore.methods.blockbuilding.TokenBlocking
import org.bd720.ercore.methods.blockrefinement.pruningmethod.CEP
import org.bd720.ercore.methods.blockrefinement.{BlockFiltering, BlockPurging}
import org.bd720.ercore.methods.datastructure.KeysCluster
import org.bd720.ercore.methods.entityclustering.EntityClusterUtils
import org.bd720.ercore.methods.entitymatching.{EntityMatching, MatchingFunctions}
import org.bd720.ercore.methods.util.Converters
object End2EndSimpleFlowSample extends ERFlow with SparkEnvSetup {
  def run(args: Array[String]) = {
    val sourceId1 = 1001
    val sourceId2 = 1002
    val spark = SparkSession.builder().getOrCreate()
    log.info("launch full end2end flow")
    val gtPath = getClass.getClassLoader.getResource("sampledata/dblpAcmIdDuplicates.mini.gen.csv").getPath
    log.info("load ground-truth from path {}", gtPath)
    val gtRdd = GroundTruthLoader.loadGroundTruth(gtPath)
    log.info("gt size is {}", gtRdd.count())
    val startIDFrom = 0
    val separator = ","
    val ep1Path = getClass.getClassLoader.getResource("sampledata/acmProfiles.mini.gen.csv").getPath
    val profileLoader = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(ep1Path))
    val ep1Rdd = profileLoader.load(ep1Path, startIDFrom, "", sourceId1)
    log.info("ep1 size is {}", ep1Rdd.count())
    val secondProfileStartIDFrom = ep1Rdd.count().toInt - 1 + startIDFrom
    val ep2Path = getClass.getClassLoader.getResource("sampledata/dblpProfiles.mini.gen.csv").getPath
    val ep2Rdd = CSVProfileLoader.loadProfilesAdvanceMode(ep2Path, secondProfileStartIDFrom, separator, header = true, sourceId = sourceId2)
    log.info("ep2 size is {}", ep2Rdd.count())
    val allEPRdd = ep1Rdd.union(ep2Rdd)
    allEPRdd.sortBy(_.id).take(5).foreach(x => log.info("all-profileId=" + x))
    val separators = Array[Int](secondProfileStartIDFrom)
    var clusters = List[KeysCluster]()
    clusters = KeysCluster(100111, List(sourceId1 + "_year", sourceId2 + "_year")) :: clusters
    clusters = KeysCluster(100112, List(sourceId1 + "_title", sourceId2 + "_title")) :: clusters
    clusters = KeysCluster(100113, List(sourceId1 + "_authors", sourceId2 + "_authors")) :: clusters
    clusters = KeysCluster(100114, List(sourceId1 + "_venue", sourceId2 + "_venue")) :: clusters
    val epBlocks = TokenBlocking.createBlocksCluster(allEPRdd, separators, clusters)
    log.info("pb-detail-bc count " + epBlocks.count() + " first " + epBlocks.first())
    epBlocks.top(5).foreach(b => log.info("ep1b is {}", b))
    val profileBlocks = Converters.blocksToProfileBlocks(epBlocks)
    log.info("pb-detail-bb count " + profileBlocks.count() + " first " + profileBlocks.first())
    val profileBlockFilter1 = BlockFiltering.blockFiltering(profileBlocks, ratio = 0.75)
    log.info("pb-detail-bf count " + profileBlockFilter1.count() + " first " + profileBlockFilter1.first())
    profileBlockFilter1.take(5).foreach(x => log.info("blockPurge=" + x))
    val abRdd1 = Converters.profilesBlockToBlocks(profileBlockFilter1, separatorIDs = separators)
    val pAbRdd1 = BlockPurging.blockPurging(abRdd1, 0.6)
    log.info("pb-detail-bp count " + pAbRdd1.count() + " first " + pAbRdd1.first())
    val broadcastVar = spark.sparkContext.broadcast(ep1Rdd.collect())
    val combinedRdd = ep2Rdd.flatMap(p2 => broadcastVar.value.map(p1 => (p1, p2)))
    combinedRdd.take(2).foreach(x => log.info("pb-detail-cb combined {}", x))
    val weRdd = combinedRdd.map(x => EntityMatching.profileMatching(x._1, x._2, MatchingFunctions.jaccardSimilarity))
    weRdd.take(2).foreach(x => log.info("pb-detail-pm weRdd=" + x))
    val connected = EntityClusterUtils.connectedComponents(weRdd)
    connected.take(5).foreach(x => log.info("pb-detail-cc connected=" + x))
    val all = connected.flatMap(x => x)
    val similarPairs = all.filter(x => x._3 >= 0.5)
    val resolvedSimPairs = similarPairs.map(x => (x._1, x._2 - secondProfileStartIDFrom, x._3))
    import spark.implicits._
    val pwd = System.getProperty("user.dir")
    resolvedSimPairs.toDF.write.csv(pwd + "/output/" + System.currentTimeMillis() + "/data.csv")
    spark.close()
  }
}
