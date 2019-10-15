package validation

import java.text.SimpleDateFormat

import cats.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}
import utils.test.InitSparkSessionFunSuite
import utils.test.schema.SchemaModels._
import validation.validations.RangeCase
import validation.Validate._
import validation.Validations._
import java.sql.Timestamp

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

  test("int min failures max") {

    val columnToCheck = Seq(
      Row(1),
      Row(20),
      Row(-20),
      Row(200),
      Row(30)
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, IntegerType))

    val dfResult=RangeCase(FIELD_1, 20, 300).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe false)
  }

  test("int min max failures") {

    val columnToCheck = Seq(
      Row(1),
      Row(20),
      Row(-20),
      Row(200),
      Row(30)
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, IntegerType))

    val dfResult=RangeCase(FIELD_1, -400, 100).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe false)
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

  test("double min failures max") {

    val columnToCheck = Seq(
      Row(1.1),
      Row(20.1),
      Row(-20.1),
      Row(200.1),
      Row(30.1)
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, DoubleType))

    val dfResult=RangeCase(FIELD_1, 20.5,300.0).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe false)
  }

  test("double min max failures") {

    val columnToCheck = Seq(
      Row(1.1),
      Row(20.1),
      Row(-20.1),
      Row(200.1),
      Row(30.1)
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, DoubleType))

    val dfResult=RangeCase(FIELD_1, -20.5,-300.0).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe false)
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

  test("float min failures max") {

    val columnToCheck = Seq(
      Row(1.1F),
      Row(20.1F),
      Row(-20.1F),
      Row(200.1F),
      Row(30.1F)
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, FloatType))

    val dfResult=RangeCase(FIELD_1,20.5F, 300.0F).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe false)
  }

  test("float min max failures") {

    val columnToCheck = Seq(
      Row(1.1F),
      Row(20.1F),
      Row(-20.1F),
      Row(200.1F),
      Row(30.1F)
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, FloatType))

    val dfResult=RangeCase(FIELD_1,-20.5F, 150.0F).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe false)
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

  test("timestamp min timestamp max") {

    val DATE_FORMAT = "dd-MM-yyyy"
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    val columnToCheck = Seq(
      Row(new Timestamp(dateFormat.parse("13-01-1991").getTime())),
      Row(new Timestamp(dateFormat.parse("13-02-1991").getTime())),
      Row(new Timestamp(dateFormat.parse("13-03-1991").getTime())),
      Row(new Timestamp(dateFormat.parse("13-04-1991").getTime())),
      Row(new Timestamp(dateFormat.parse("13-05-1991").getTime()))
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, TimestampType))

    val dfResult=RangeCase(FIELD_1,
      new Timestamp(dateFormat.parse("12-02-1990").getTime()),
      new Timestamp(dateFormat.parse("13-06-1991").getTime())).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe true)
  }

  test("timestamp min failures timestamp max") {

    val DATE_FORMAT = "dd-MM-yyyy"
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    val columnToCheck = Seq(
      Row(new Timestamp(dateFormat.parse("13-01-1991").getTime())),
      Row(new Timestamp(dateFormat.parse("13-02-1991").getTime())),
      Row(new Timestamp(dateFormat.parse("13-03-1991").getTime())),
      Row(new Timestamp(dateFormat.parse("13-04-1991").getTime())),
      Row(new Timestamp(dateFormat.parse("13-05-1991").getTime()))
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, TimestampType))

    val dfResult=RangeCase(FIELD_1,
      new Timestamp(dateFormat.parse("12-02-1991").getTime()),
      new Timestamp(dateFormat.parse("13-06-1991").getTime())).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe false)
  }

  test("timestamp min timestamp max failures") {

    val DATE_FORMAT = "dd-MM-yyyy"
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    val columnToCheck = Seq(
      Row(new Timestamp(dateFormat.parse("13-01-1991").getTime())),
      Row(new Timestamp(dateFormat.parse("13-02-1991").getTime())),
      Row(new Timestamp(dateFormat.parse("13-03-1991").getTime())),
      Row(new Timestamp(dateFormat.parse("13-04-1991").getTime())),
      Row(new Timestamp(dateFormat.parse("13-05-1991").getTime()))
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, TimestampType))

    val dfResult=RangeCase(FIELD_1,
      new Timestamp(dateFormat.parse("12-02-1990").getTime()),
      new Timestamp(dateFormat.parse("13-06-1989").getTime())).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe false)
  }

  test("sql date min sql date max") {

    val DATE_FORMAT = "dd-MM-yyyy"
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    val columnToCheck = Seq(
      Row(new java.sql.Date(dateFormat.parse("13-01-1991").getTime())),
      Row(new java.sql.Date(dateFormat.parse("13-02-1991").getTime())),
      Row(new java.sql.Date(dateFormat.parse("13-03-1991").getTime())),
      Row(new java.sql.Date(dateFormat.parse("13-04-1991").getTime())),
      Row(new java.sql.Date(dateFormat.parse("13-05-1991").getTime()))
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, DateType))

    val dfResult=RangeCase(FIELD_1,
      new java.sql.Date(dateFormat.parse("12-02-1990").getTime()),
      new java.sql.Date(dateFormat.parse("13-06-1999").getTime())).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe true)
  }

  test("sql date min failures sql date max") {

    val DATE_FORMAT = "dd-MM-yyyy"
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    val columnToCheck = Seq(
      Row(new java.sql.Date(dateFormat.parse("13-01-1991").getTime())),
      Row(new java.sql.Date(dateFormat.parse("13-02-1991").getTime())),
      Row(new java.sql.Date(dateFormat.parse("13-03-1991").getTime())),
      Row(new java.sql.Date(dateFormat.parse("13-04-1991").getTime())),
      Row(new java.sql.Date(dateFormat.parse("13-05-1991").getTime()))
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, DateType))

    val dfResult=RangeCase(FIELD_1,
      new java.sql.Date(dateFormat.parse("12-02-1992").getTime()),
      new java.sql.Date(dateFormat.parse("13-06-1999").getTime())).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe false)
  }

  test("sql date min sql date max failures") {

    val DATE_FORMAT = "dd-MM-yyyy"
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    val columnToCheck = Seq(
      Row(new java.sql.Date(dateFormat.parse("13-01-1991").getTime())),
      Row(new java.sql.Date(dateFormat.parse("13-02-1991").getTime())),
      Row(new java.sql.Date(dateFormat.parse("13-03-1991").getTime())),
      Row(new java.sql.Date(dateFormat.parse("13-04-1991").getTime())),
      Row(new java.sql.Date(dateFormat.parse("13-05-1991").getTime()))
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, DateType))

    val dfResult=RangeCase(FIELD_1,
      new java.sql.Date(dateFormat.parse("12-02-1990").getTime()),
      new java.sql.Date(dateFormat.parse("13-06-1989").getTime())).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe false)
  }

  test("util date min util date max") {

    val DATE_FORMAT = "dd-MM-yyyy"
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    val columnToCheck = Seq(
      Row(new java.sql.Date(dateFormat.parse("13-01-1991").getTime)),
      Row(new java.sql.Date(dateFormat.parse("13-02-1991").getTime)),
      Row(new java.sql.Date(dateFormat.parse("13-03-1991").getTime)),
      Row(new java.sql.Date(dateFormat.parse("13-04-1991").getTime)),
      Row(new java.sql.Date(dateFormat.parse("13-05-1991").getTime))
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, DateType))

    val dfResult=RangeCase(FIELD_1,
      new java.util.Date(dateFormat.parse("12-02-1990").getTime),
      new java.util.Date(dateFormat.parse("13-06-1999").getTime)).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe true)
  }

  test("util date min failures util date max") {

    val DATE_FORMAT = "dd-MM-yyyy"
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    val columnToCheck = Seq(
      Row(new java.sql.Date(dateFormat.parse("13-01-1991").getTime)),
      Row(new java.sql.Date(dateFormat.parse("13-02-1991").getTime)),
      Row(new java.sql.Date(dateFormat.parse("13-03-1991").getTime)),
      Row(new java.sql.Date(dateFormat.parse("13-04-1991").getTime)),
      Row(new java.sql.Date(dateFormat.parse("13-05-1991").getTime))
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, DateType))

    val dfResult=RangeCase(FIELD_1,
      new java.util.Date(dateFormat.parse("12-02-1992").getTime),
      new java.util.Date(dateFormat.parse("13-06-1999").getTime)).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe false)
  }

  test("util date min util date max failures") {

    val DATE_FORMAT = "dd-MM-yyyy"
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    val columnToCheck = Seq(
      Row(new java.sql.Date(dateFormat.parse("13-01-1991").getTime)),
      Row(new java.sql.Date(dateFormat.parse("13-02-1991").getTime)),
      Row(new java.sql.Date(dateFormat.parse("13-03-1991").getTime)),
      Row(new java.sql.Date(dateFormat.parse("13-04-1991").getTime)),
      Row(new java.sql.Date(dateFormat.parse("13-05-1991").getTime))
    )

    val df: DataFrame =
      createDataFrame(columnToCheck, inputSchema(1, DateType))

    val dfResult=RangeCase(FIELD_1,
      new java.util.Date(dateFormat.parse("12-02-1990").getTime),
      new java.util.Date(dateFormat.parse("13-06-1989").getTime)).testing(df)

    dfResult.isRight shouldBe true
    dfResult.map(result => result shouldBe false)
  }

//  test("util date min util date NO max") {
//
//    val DATE_FORMAT = "dd-MM-yyyy"
//    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
//    val columnToCheck = Seq(
//      Row(new java.sql.Date(dateFormat.parse("13-01-1991").getTime)),
//      Row(new java.sql.Date(dateFormat.parse("13-02-1991").getTime)),
//      Row(new java.sql.Date(dateFormat.parse("13-03-1991").getTime)),
//      Row(new java.sql.Date(dateFormat.parse("13-04-1991").getTime)),
//      Row(new java.sql.Date(dateFormat.parse("13-05-1991").getTime))
//    )
//
//    val df: DataFrame =
//      createDataFrame(columnToCheck, inputSchema(1, DateType))
//
//    val dfResult=RangeCase(field=FIELD_1,
//      min=new java.util.Date(dateFormat.parse("12-02-1990").getTime)).testing(df)
//
//    dfResult.isRight shouldBe true
//    dfResult.map(result => result shouldBe false)
//  }
}
