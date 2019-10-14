package validation.validations.Column

import org.apache.spark.sql.DataFrame
import validation.Validate
import validation.Validate._
import validation.errors.{InvalidType, ValidationError}
import cats.implicits._
import shapeless.Coproduct
import validation.validations.RangeCase
import utils.NumberLike.NumberLike.ops._
import utils.NumberLike.NumberLikeConverter
import utils.NumberLike.NumberLikeType.NumberLikeType
import validation.validations._

object RangeImp {

  object RangeInstance extends RangeInstance

  trait RangeInstance {
    implicit val RangeValidationInt: Validate[RangeCase[Int]] =
      validateInstance((op: RangeCase[Int], col: DataFrame, name: String) => validation(op, col, name))

    implicit val RangeValidationFloat: Validate[RangeCase[Float]] =
      validateInstance((op: RangeCase[Float], col: DataFrame, name: String) => validation(op, col, name))

    implicit val RangeValidationDouble: Validate[RangeCase[Double]] =
      validateInstance((op: RangeCase[Double], col: DataFrame, name: String) => validation(op, col, name))
  }

  private def validation[A](op: RangeCase[A], col: DataFrame, name: String): Either[ValidationError, Boolean]= {


    def NumberLikeConversion[A](element: A): Either[ValidationError, NumberLikeType]={
      element match {
        case elem: Int => Coproduct[NumberLikeType](elem).asRight
        case elem: Double => Coproduct[NumberLikeType](elem).asRight
        case elem: Float => Coproduct[NumberLikeType](elem).asRight
        case _ => InvalidType("Range", "min/max", " ").asLeft
      }
    }

    def assertType[B](minimum: NumberLikeType, maximum: NumberLikeType): Either[ValidationError, NumberLikeType] ={
      maximum.asRight
    }

    val minimumCol=NumberLikeConversion(col.groupBy().min(name).head().get(0))
    val maximumCol=NumberLikeConversion(col.groupBy().max(name).head().get(0))


    val minimum: Either[ValidationError, Boolean]=minimumCol.flatMap(min => assertType(op.min, min).map{
      result=> op.min.lessThenOrEqual(result)
    })
    val maximum: Either[ValidationError, Boolean]=maximumCol.flatMap(max => assertType(op.max, max).map{
      result =>  op.max.moreThenOrEqual(result)
    })

    minimum.flatMap(right => maximum.map(right2 => right && right2))
  }
}
