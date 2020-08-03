package org.wumiguo.ser.methods.similarityjoins.common.ed
import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
class CommonEdFunctionsTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "getQgramsTf v1 with abnormal input" in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, Array[(String, Int)])](
      (1, Array(("hello", 2), ("bd", 1), ("spark", 4), ("program", 0))),
      (2, Array(("world", 5), ("tech", 3), ("test", 2))),
      (3, Array(("hello", 3))),
      (4, Array()),
      (5, Array(("spark", 4), ("bd", 1), ("bd", 6)))
    ))
    val map = CommonEdFunctions.getQgramsTf(docsRdd)
    map.foreach(x => println(x))
    assertResult(3)(map.get("bd").get)
    assertResult(1)(map.get("program").get)
  }
  it should "getQgramsTf v2 with normal input with equal string size in doc array" in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, Array[(String, Int)])](
      (1, Array(("hell", 0), ("o bi", 1), ("gdat", 2), ("a tec", 3), ("tech", 4))), //"hello bigdata tech"
      (2, Array(("it's", 0), (" all", 1), (" abo", 2), ("ut t", 3), ("tech", 4))), //"it's all about tech"
      (3, Array(("samp", 0), ("le d", 1), ("data", 2))), //"sample data",
      (4, Array(("hi", 0))) //"hi"
    ))
    val mapRdd = CommonEdFunctions.getQgramsTf(docsRdd)
    mapRdd.foreach(x => println("qgram=" + x))
    assertResult(None)(mapRdd.get("bd"))
    assertResult(1)(mapRdd.get("it's").get)
    assertResult(2)(mapRdd.get("tech").get)
    assertResult(1)(mapRdd.get("hi").get)
  }
  it should "getQgrams" in {
    val str = "hello bigdata-tech, handle super big volume of data in the world! bigdata is a great tech in the world"
    val qgramSize = 6
    val result = CommonEdFunctions.getQgrams(str, qgramSize)
    result.foreach(x => println("gram=" + x))
    assertResult(("hello ", 0))(result.head)
    assertResult((" world", result.size - 1))(result.last)
  }
  it should "getSortedQgrams2 " in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, String, Array[(String, Int)])](
      (1, "hello bigdata tech", Array(("hell", 0), ("o bi", 1), ("gdat", 2), ("a tec", 3), ("tech", 4))),
      (2, "it's all about tech", Array(("it's", 0), (" all", 1), (" abo", 2), ("ut t", 3), ("tech", 4))),
      (3, "sample data", Array(("samp", 0), ("le d", 1), ("data", 2))),
      (4, "hi", Array(("hi", 0))),
      (6, "all data", Array(("all ", 0), ("data", 0))),
      (7, "tech", Array(("tech", 0))),
      (8, "uuou", Array(("uuou", 0)))
    ))
    val sortedQg = CommonEdFunctions.getSortedQgrams2(docsRdd)
    sortedQg.foreach(x => println("sorted=" + x._1 + "," + x._2 + "," + x._3.toList))
  }
}
