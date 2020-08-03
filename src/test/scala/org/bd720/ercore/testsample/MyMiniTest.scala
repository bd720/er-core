package org.wumiguo.ser.testsample
import org.scalatest.FeatureSpec
class MyMiniTest extends FeatureSpec {
  scenario("A simple test") {
    val a = 12
    assert(a * 3 == 36)
  }
}