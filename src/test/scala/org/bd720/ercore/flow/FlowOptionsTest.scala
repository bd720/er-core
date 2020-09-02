package org.bd720.ercore.flow
import org.scalatest.FlatSpec
class FlowOptionsTest extends FlatSpec {
  it should "load flow options map" in {
    val args = Array[String](
      "optionSize=4",
      "option0=type:SSJoin",
      "option1=q:2",
      "option2=threshold:2",
      "option3=algorithm:EDJoin"
    )
    val options = FlowOptions.getOptions(args)
    assertResult(Some("SSJoin"))(options.get("type"))
    assertResult("2")(options.get("q").get)
    assertResult("2")(options.get("threshold").get)
    assertResult("EDJoin")(options.get("algorithm").get)
  }
}
