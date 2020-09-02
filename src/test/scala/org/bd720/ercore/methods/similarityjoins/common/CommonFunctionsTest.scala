package org.bd720.ercore.methods.similarityjoins.common
import org.scalatest.FlatSpec
import org.bd720.ercore.common.SparkEnvSetup
import org.bd720.ercore.methods.datastructure.{KeyValue, Profile}
import scala.collection.mutable
class CommonFunctionsTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "extract field value from profile " in {
    val pRdd = spark.sparkContext.makeRDD(Seq(
      Profile(0, mutable.MutableList(KeyValue("year", "0"), KeyValue("title", "The WASA2 object-oriented workflow management system"), KeyValue("venue", "International Conference on Management of Data"), KeyValue("authors", "Gottfried Vossen, Mathias Weske")), "na1", 1001),
      Profile(1, mutable.MutableList(KeyValue("authors", "0"), KeyValue("year", "1999"), KeyValue("title", "Semantic Integration of Environmental Models"), KeyValue("venue", "SIGMOD Record")), "na2", 2002))
    )
    val vRdd = CommonFunctions.extractField(pRdd, "title").sortBy(_._1)
    vRdd.foreach(x => println("values=" + x))
    assert(2 == vRdd.count())
    assertResult((0, "the wasa2 object-oriented workflow management system"))(vRdd.first())
  }
  it should "extract all field value from profile " in {
    val pRdd = spark.sparkContext.makeRDD(Seq(
      Profile(0, mutable.MutableList(KeyValue("year", "0"), KeyValue("title", "The WASA2 object-oriented workflow management system"), KeyValue("venue", "International Conference on Management of Data"), KeyValue("authors", "Gottfried Vossen, Mathias Weske")), "na1", 1001),
      Profile(1, mutable.MutableList(KeyValue("authors", "0"), KeyValue("year", "1999"), KeyValue("title", "Semantic Integration of Environmental Models"), KeyValue("venue", "SIGMOD Record")), "na2", 2002))
    )
    val vRdd = CommonFunctions.extractAllFields(pRdd).sortBy(_._1)
    vRdd.foreach(x => println("values=" + x))
    assert(2 == vRdd.count())
    assertResult((0, "0 the wasa2 object-oriented workflow management system international conference on management of data gottfried vossen, mathias weske"))(vRdd.first())
  }
}
