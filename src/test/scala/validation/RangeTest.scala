package validation

import cats.implicits._
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType}
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.LocalDate
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import shapeless.{:+:, CNil, Coproduct}
import utils.NumberLike.NumberLike
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaModels._
import validation.validations.RangeCase
import validation.Validate._
import validation.Validations._
import utils.NumberLike.NumberLike._
import utils.NumberLike.NumberLikeType.NumberLikeType
import utils.NumberLike.NumberLike.ops._

@RunWith(classOf[JUnitRunner])
class RangeTest
    extends InitSparkSessionFunSuite
    with GivenWhenThen
    with Matchers {
  test("int min max") {

    val columnToCheck = Seq(
      Row(1),
      Row(20),
      Row(-20),
      Row(200),
      Row(30)
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, IntegerType))

    val dfResult=RangeCase(FIELD_1, -30, 300).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe true)
  }

  test("double min max") {

    val columnToCheck = Seq(
      Row(1.1),
      Row(20.1),
      Row(-20.1),
      Row(200.1),
      Row(30.1)
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, DoubleType))

    val dfResult=RangeCase(FIELD_1, -20.5,300.0).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe true)
  }

  test("float min max") {

    val columnToCheck = Seq(
      Row(1.1F),
      Row(20.1F),
      Row(-20.1F),
      Row(200.1F),
      Row(30.1F)
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, FloatType))

    val dfResult=RangeCase(FIELD_1,-20.5F, 300.0F).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe true)
  }

  test("float min double max") {

    val columnToCheck = Seq(
      Row(1.1F),
      Row(20.1F),
      Row(-20.1F),
      Row(200.1F),
      Row(30.1F)
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, FloatType))

    val dfResult=RangeCase(FIELD_1,-20.5F, 300.4D).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe true)
  }

  test("int min double max column float") {

    val columnToCheck = Seq(
      Row(1.1F),
      Row(20.1F),
      Row(-20.1F),
      Row(200.1F),
      Row(30.1F)
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, FloatType))

    val dfResult=RangeCase(FIELD_1,-20, 300.6D).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe true)
  }

}
