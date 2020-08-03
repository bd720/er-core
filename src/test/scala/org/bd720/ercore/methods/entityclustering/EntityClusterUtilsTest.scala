package org.wumiguo.ser.methods.entityclustering
import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.datastructure.WeightedEdge
class EntityClusterUtilsTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "connectedComponents " in {
    val weRdd = spark.sparkContext.parallelize(Seq(
      WeightedEdge(0, 0, 0.058823529411764705), WeightedEdge(1, 0, 0.08108108108108109), WeightedEdge(2, 0, 0.06976744186046512),
      WeightedEdge(0, 1, 0.625), WeightedEdge(1, 1, 0.05555555555555555), WeightedEdge(2, 1, 0.07317073170731707),
      WeightedEdge(0, 2, 0.037037037037037035), WeightedEdge(1, 2, 0.06666666666666667), WeightedEdge(2, 2, 0.02702702702702703)
    ))
    val connected = EntityClusterUtils.connectedComponents(weRdd)
    connected.foreach(x => println("connectedComp=" + x))
  }
}
