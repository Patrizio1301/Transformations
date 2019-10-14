package utils.NumerikalLikeTests

import org.junit.runner.RunWith
import org.scalatest.{GivenWhenThen, Matchers}
import org.scalatest.junit.JUnitRunner
import utils.test.InitSparkSessionFunSuite
import utils.NumberLike.NumberLike._

@RunWith(classOf[JUnitRunner])
class NumberLikeTests
  extends InitSparkSessionFunSuite
    with GivenWhenThen
    with Matchers {
  test("int implicits") {
    val a=5
    val b=10
    a.toString

  }
}