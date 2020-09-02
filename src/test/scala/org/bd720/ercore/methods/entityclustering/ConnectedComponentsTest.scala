package org.bd720.ercore.methods.entityclustering
import org.scalatest.FlatSpec
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.methods.datastructure.WeightedEdge
class ConnectedComponentsTest extends FlatSpec with SparkEnvSetup with Serializable {
  val spark = createLocalSparkSession(getClass.getName)
  it should "run v1" in {
    val weRdd = spark.sparkContext.parallelize(Seq(
      WeightedEdge(3, 3, 0.058823529411764705), WeightedEdge(1, 0, 0.08108108108108109), WeightedEdge(2, 0, 0.06976744186046512),
      WeightedEdge(0, 1, 0.625), WeightedEdge(1, 1, 0.05555555555555555), WeightedEdge(2, 1, 0.07317073170731707),
      WeightedEdge(0, 2, 0.037037037037037035), WeightedEdge(1, 2, 0.06666666666666667), WeightedEdge(2, 2, 0.02702702702702703)
    ))
    val weRddCount = weRdd.count()
    val ccNodes = ConnectedComponents.run(weRdd, 200)
    val _1 = ccNodes._1
    val _2 = ccNodes._2
    val _3 = ccNodes._3
    log.info("_2=" + _2 + ", _3=" + _3)
    log.info("count=" + _1.count() + ", weC=" + weRddCount)
    _1.foreach(x => println("nodeItem=" + x))
    assert(4 == _1.count())
    assertResult(List[(Long, Long)]((0, 0), (1, 0), (2, 0), (3, 3)))(_1.toLocalIterator.toList)
  }
  it should "run v2" in {
    val weRdd = spark.sparkContext.parallelize(Seq(
      WeightedEdge(3, 3, 0.058823529411764705), WeightedEdge(1, 0, 0.08108108108108109),
      WeightedEdge(2, 0, 0.06976744186046512), WeightedEdge(0, 1, 0.625)
    ))
    val weRddCount = weRdd.count()
    val ccNodes = ConnectedComponents.run(weRdd, 200)
    val _1 = ccNodes._1
    val _2 = ccNodes._2
    val _3 = ccNodes._3
    log.info("_2=" + _2 + ", _3=" + _3)
    log.info("count=" + _1.count() + ", weC=" + weRddCount)
    _1.foreach(x => println("nodeItem=" + x))
    assert(4 == _1.count())
    assertResult(List[(Long, Long)]((0, 0), (1, 0), (2, 0), (3, 3)))(_1.toLocalIterator.toList)
  }
  it should "run v3" in {
    val weRdd = spark.sparkContext.parallelize(Seq(
      WeightedEdge(0, 1, 0.058823529411764705),
      WeightedEdge(1, 0, 0.08108108108108109)
    ))
    val weRddCount = weRdd.count()
    val ccNodes = ConnectedComponents.run(weRdd, 200)
    val _1 = ccNodes._1
    val _2 = ccNodes._2
    val _3 = ccNodes._3
    log.info("_2=" + _2 + ", _3=" + _3)
    log.info("count=" + _1.count() + ", weC=" + weRddCount)
    _1.foreach(x => println("nodeItem=" + x))
    assert(2 == _1.count())
    assertResult(List[(Long, Long)]((0, 0), (1, 0)))(_1.toLocalIterator.toList)
  }
  it should "run v4" in {
    val weRdd = spark.sparkContext.parallelize(Seq(
      WeightedEdge(0, 0, 0.058823529411764705)
    ))
    val weRddCount = weRdd.count()
    val ccNodes = ConnectedComponents.run(weRdd, 200)
    val _1 = ccNodes._1
    val _2 = ccNodes._2
    val _3 = ccNodes._3
    log.info("_2=" + _2 + ", _3=" + _3)
    log.info("count=" + _1.count() + ", weC=" + weRddCount)
    _1.foreach(x => println("nodeItem=" + x))
    assert(1 == _1.count())
    assertResult((0, 0))(_1.first())
  }
  it should "run v5" in {
    val weRdd = spark.sparkContext.parallelize(Seq(
      WeightedEdge(0, 1, 0.058823529411764705)
    ))
    val weRddCount = weRdd.count()
    val ccNodes = ConnectedComponents.run(weRdd, 200)
    val _1 = ccNodes._1
    val _2 = ccNodes._2
    val _3 = ccNodes._3
    log.info("_2=" + _2 + ", _3=" + _3)
    log.info("count=" + _1.count() + ", weC=" + weRddCount)
    _1.foreach(x => println("nodeItem=" + x))
    assert(2 == _1.count())
    assertResult(List[(Long, Long)]((0, 0), (1, 0)))(_1.toLocalIterator.toList)
  }
  it should "run v6" in {
    val weRdd = spark.sparkContext.parallelize(Seq(
      WeightedEdge(1, 0, 0.058823529411764705),
      WeightedEdge(2, 1, 0.058823529411764705)
    ))
    val weRddCount = weRdd.count()
    val ccNodes = ConnectedComponents.run(weRdd, 200)
    val _1 = ccNodes._1
    val _2 = ccNodes._2
    val _3 = ccNodes._3
    log.info("_2=" + _2 + ", _3=" + _3)
    log.info("count=" + _1.count() + ", weC=" + weRddCount)
    _1.foreach(x => println("nodeItem=" + x))
    assert(3 == _1.count())
    assertResult(List[(Long, Long)]((0, 0), (1, 0), (2, 0)))(_1.toLocalIterator.toList)
  }
  it should "run v7" in {
    val weRdd = spark.sparkContext.parallelize(Seq(
      WeightedEdge(1, 1, 0.058823529411764705),
      WeightedEdge(2, 2, 0.058823529411764705)
    ))
    val weRddCount = weRdd.count()
    val ccNodes = ConnectedComponents.run(weRdd, 200)
    val _1 = ccNodes._1
    val _2 = ccNodes._2
    val _3 = ccNodes._3
    log.info("_2=" + _2 + ", _3=" + _3)
    log.info("count=" + _1.count() + ", weC=" + weRddCount)
    _1.foreach(x => println("nodeItem=" + x))
    assert(2 == _1.count())
    assertResult(List[(Long, Long)]((1, 1), (2, 2)))(_1.toLocalIterator.toList)
  }
  it should "run v8" in {
    val weRdd = spark.sparkContext.parallelize(Seq(
      WeightedEdge(2, 1, 0.058823529411764705),
      WeightedEdge(3, 4, 0.058823529411764705)
    ))
    val weRddCount = weRdd.count()
    val ccNodes = ConnectedComponents.run(weRdd, 200)
    val _1 = ccNodes._1
    val _2 = ccNodes._2
    val _3 = ccNodes._3
    log.info("_2=" + _2 + ", _3=" + _3)
    log.info("count=" + _1.count() + ", weC=" + weRddCount)
    _1.foreach(x => println("nodeItem=" + x))
    assert(4 == _1.count())
    assertResult(List[(Long, Long)]((4, 3), (1, 1), (2, 1), (3, 3)))(_1.toLocalIterator.toList)
  }
}
