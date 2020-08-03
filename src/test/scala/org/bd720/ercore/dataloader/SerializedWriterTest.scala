package org.wumiguo.ser.dataloader
import java.util
import org.scalatest.FlatSpec
import org.wumiguo.ser.model.IdDuplicates
import scala.collection.mutable
class SerializedWriterTest extends FlatSpec {
  it should " write IdDuplicateSet to file " in {
    val testOutputDir = "test-output"
    val outputDir = getClass().getClassLoader.getResource(testOutputDir).getPath
    println(s"test output dir is $testOutputDir")
    val gtOnCp = outputDir + "/dblpVsAcmGt-IdDuplicateSet"
    val gtRules = new java.util.HashSet[IdDuplicates]()
    gtRules.add(IdDuplicates(2123, 0))
    gtRules.add(IdDuplicates(1821, 1345))
    SerializedWriter.serializedGroundTruth(gtOnCp, gtRules)
    assert(1 == 1)
  }
  it should " write EntityProfiles to file " in {
  }
}
