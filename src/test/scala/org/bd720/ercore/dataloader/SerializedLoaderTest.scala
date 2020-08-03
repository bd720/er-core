package org.wumiguo.ser.dataloader
import org.scalatest.FlatSpec
import org.wumiguo.ser.testutil.TestDirs
class SerializedLoaderTest extends FlatSpec {
  it should "all good to load good serialized data" in {
    val gtFile = TestDirs.resolveDataPath("/serialized/dblpVsAcmGt-IdDuplicateSet")
    val data = SerializedLoader.loadSerializedGroundtruth(gtFile)
    assert(data != null)
    assert(data.size() == 2)
  }
}
