package utils.NumberLikeTest

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.junit.runner.RunWith
import org.scalatest.{GivenWhenThen, Matchers}
import org.scalatest.junit.JUnitRunner
import shapeless.Coproduct
import utils.test.InitSparkSessionFunSuite
import utils.NumberLike.NumberLike._
import utils.NumberLike.NumberLike.ops._
import utils.NumberLike.NumberLikeType.NumberLikeType
import cats.implicits._

@RunWith(classOf[JUnitRunner])
class NumberLikeTest
  extends InitSparkSessionFunSuite with GivenWhenThen with Matchers {

  test("int") {
    5.lessThanOrEqual(10).map(result => result shouldBe true)
    10.lessThanOrEqual(5).map(result => result shouldBe false)
    5.moreThanOrEqual(10).map(result => result shouldBe false)
    10.moreThanOrEqual(5).map(result => result shouldBe true)

    val a=Coproduct[NumberLikeType](5)
    val b=Coproduct[NumberLikeType](10)

    a.lessThanOrEqual(b).map(result => result shouldBe true)
    b.lessThanOrEqual(a).map(result => result shouldBe false)
    a.moreThanOrEqual(b).map(result => result shouldBe false)
    b.moreThanOrEqual(a).map(result => result shouldBe true)
  }

  test("double") {
    5.0D.lessThanOrEqual(10.0D).map(result => result shouldBe true)
    10.0D.lessThanOrEqual(5.0D).map(result => result shouldBe false)
    5.0D.moreThanOrEqual(10.0D).map(result => result shouldBe false)
    10.0D.moreThanOrEqual(5.0D).map(result => result shouldBe true)

    val a=Coproduct[NumberLikeType](5.0D)
    val b=Coproduct[NumberLikeType](10.0D)

    a.lessThanOrEqual(b).map(result => result shouldBe true)
    b.lessThanOrEqual(a).map(result => result shouldBe false)
    a.moreThanOrEqual(b).map(result => result shouldBe false)
    b.moreThanOrEqual(a).map(result => result shouldBe true)
  }

  test("float") {
    5.0F.lessThanOrEqual(10.0F).map(result => result shouldBe true)
    10.0F.lessThanOrEqual(5.0F).map(result => result shouldBe false)
    5.0F.moreThanOrEqual(10.0F).map(result => result shouldBe false)
    10.0F.moreThanOrEqual(5.0F).map(result => result shouldBe true)

    val a=Coproduct[NumberLikeType](5.0F)
    val b=Coproduct[NumberLikeType](10.0F)

    a.lessThanOrEqual(b).map(result => result shouldBe true)
    b.lessThanOrEqual(a).map(result => result shouldBe false)
    a.moreThanOrEqual(b).map(result => result shouldBe false)
    b.moreThanOrEqual(a).map(result => result shouldBe true)
  }

  test("int - double") {
    5.lessThanOrEqual(10.0D).map(result => result shouldBe true)
    10.lessThanOrEqual(5.0D).map(result => result shouldBe false)
    5.moreThanOrEqual(10.0D).map(result => result shouldBe false)
    10.moreThanOrEqual(5.0D).map(result => result shouldBe true)
    5.0D.lessThanOrEqual(10).map(result => result shouldBe true)
    10.0D.lessThanOrEqual(5).map(result => result shouldBe false)
    5.0D.moreThanOrEqual(10).map(result => result shouldBe false)
    10.0D.moreThanOrEqual(5).map(result => result shouldBe true)

    val a=Coproduct[NumberLikeType](5)
    val b=Coproduct[NumberLikeType](10)
    val c=Coproduct[NumberLikeType](5.0D)
    val d=Coproduct[NumberLikeType](10.0D)

    a.lessThanOrEqual(d).map(result => result shouldBe true)
    b.lessThanOrEqual(c).map(result => result shouldBe false)
    a.moreThanOrEqual(d).map(result => result shouldBe false)
    b.moreThanOrEqual(c).map(result => result shouldBe true)
    c.lessThanOrEqual(b).map(result => result shouldBe true)
    d.lessThanOrEqual(a).map(result => result shouldBe false)
    c.moreThanOrEqual(b).map(result => result shouldBe false)
    d.moreThanOrEqual(a).map(result => result shouldBe true)
  }

  test("int - float") {
    5.lessThanOrEqual(10.0F).map(result => result shouldBe true)
    10.lessThanOrEqual(5.0F).map(result => result shouldBe false)
    5.moreThanOrEqual(10.0F).map(result => result shouldBe false)
    10.moreThanOrEqual(5.0F).map(result => result shouldBe true)
    5.0F.lessThanOrEqual(10).map(result => result shouldBe true)
    10.0F.lessThanOrEqual(5).map(result => result shouldBe false)
    5.0F.moreThanOrEqual(10).map(result => result shouldBe false)
    10.0F.moreThanOrEqual(5).map(result => result shouldBe true)

    val a=Coproduct[NumberLikeType](5)
    val b=Coproduct[NumberLikeType](10)
    val c=Coproduct[NumberLikeType](5.0F)
    val d=Coproduct[NumberLikeType](10.0F)

    a.lessThanOrEqual(d).map(result => result shouldBe true)
    b.lessThanOrEqual(c).map(result => result shouldBe false)
    a.moreThanOrEqual(d).map(result => result shouldBe false)
    b.moreThanOrEqual(c).map(result => result shouldBe true)
    c.lessThanOrEqual(b).map(result => result shouldBe true)
    d.lessThanOrEqual(a).map(result => result shouldBe false)
    c.moreThanOrEqual(b).map(result => result shouldBe false)
    d.moreThanOrEqual(a).map(result => result shouldBe true)
  }

  test("double - float") {
    5.0D.lessThanOrEqual(10.0F).map(result => result shouldBe true)
    10.0D.lessThanOrEqual(5.0F).map(result => result shouldBe false)
    5.0D.moreThanOrEqual(10.0F).map(result => result shouldBe false)
    10.0D.moreThanOrEqual(5.0F).map(result => result shouldBe true)
    5.0F.lessThanOrEqual(10.0D).map(result => result shouldBe true)
    10.0F.lessThanOrEqual(5.0D).map(result => result shouldBe false)
    5.0F.moreThanOrEqual(10.0D).map(result => result shouldBe false)
    10.0F.moreThanOrEqual(5.0D).map(result => result shouldBe true)

    val a=Coproduct[NumberLikeType](5.0D)
    val b=Coproduct[NumberLikeType](10.0D)
    val c=Coproduct[NumberLikeType](5.0F)
    val d=Coproduct[NumberLikeType](10.0F)

    a.lessThanOrEqual(d).map(result => result shouldBe true)
    b.lessThanOrEqual(c).map(result => result shouldBe false)
    a.moreThanOrEqual(d).map(result => result shouldBe false)
    b.moreThanOrEqual(c).map(result => result shouldBe true)
    c.lessThanOrEqual(b).map(result => result shouldBe true)
    d.lessThanOrEqual(a).map(result => result shouldBe false)
    c.moreThanOrEqual(b).map(result => result shouldBe false)
    d.moreThanOrEqual(a).map(result => result shouldBe true)
  }

  val DATE_FORMAT = "dd-MM-yyyy"
  val dateFormat = new SimpleDateFormat(DATE_FORMAT)

  test("timestamp") {
    val a=new Timestamp(dateFormat.parse("13-01-1991").getTime)
    val b=new Timestamp(dateFormat.parse("15-02-1992").getTime)

    a.lessThanOrEqual(b).map(result => result shouldBe true)
    b.lessThanOrEqual(a).map(result => result shouldBe false)
    a.moreThanOrEqual(b).map(result => result shouldBe false)
    b.moreThanOrEqual(a).map(result => result shouldBe true)
  }

  test("Error test: timestamp int") {
    val a=new Timestamp(dateFormat.parse("13-01-1991").getTime)
    val b=Coproduct[NumberLikeType](1)
    val c=Coproduct[NumberLikeType](a)

    c.lessThanOrEqual(b).isLeft shouldBe true
  }
}