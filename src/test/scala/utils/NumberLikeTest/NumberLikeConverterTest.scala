package utils.NumberLikeConverterTest

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.junit.runner.RunWith
import org.scalatest.{GivenWhenThen, Matchers}
import org.scalatest.junit.JUnitRunner
import shapeless.Coproduct
import utils.test.InitSparkSessionFunSuite
import utils.NumberLike.NumberLike._
import utils.NumberLike.NumberLikeConverter
import utils.NumberLike.NumberLikeConverter.DoubleToNumberLike._
import utils.NumberLike.NumberLikeConverter.DoubleToNumberLike._
import utils.NumberLike.NumberLikeConverter.DoubleToNumberLike._
import utils.NumberLike.NumberLikeType.NumberLikeType

@RunWith(classOf[JUnitRunner])
class NumberLikeConverterTest extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("int to NumberLikeType"){
    implicitly[NumberLikeConverter[Int]].apply(5) shouldBe Coproduct[NumberLikeType](5)
    implicitly[NumberLikeConverter[Double]].apply(5.0D) shouldBe Coproduct[NumberLikeType](5.0D)
    implicitly[NumberLikeConverter[Int]].apply(5) shouldBe Coproduct[NumberLikeType](5)
    implicitly[NumberLikeConverter[Int]].apply(5) shouldBe Coproduct[NumberLikeType](5)
    implicitly[NumberLikeConverter[Int]].apply(5) shouldBe Coproduct[NumberLikeType](5)
    implicitly[NumberLikeConverter[Int]].apply(5) shouldBe Coproduct[NumberLikeType](5)
  }

}
