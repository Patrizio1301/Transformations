package validation.validations.Column

import org.apache.spark.sql.DataFrame
import validation.Validate
import validation.Validate._
import validation.errors.{InvalidType, ValidationError}
import cats.implicits._
import shapeless.Coproduct
import validation.validations.RangeCase
import utils.NumberLike.NumberLike.ops._
import org.apache.spark.sql.functions._
import utils.NumberLike.NumberLikeType.NumberLikeType

object RangeImp {

  object RangeInstance extends RangeInstance

  trait RangeInstance {
    implicit val RangeValidationInt: Validate[RangeCase] =
      validateInstance((op: RangeCase, col: DataFrame, name: String) => validation(op, col, name))

  }

  private def validation[A](op: RangeCase, col: DataFrame, name: String): Either[ValidationError, Boolean]= {


    def NumberLikeConversion[A](element: A): Either[ValidationError, NumberLikeType]={
      element match {
        case elem: Int => Coproduct[NumberLikeType](elem).asRight
        case elem: Double => Coproduct[NumberLikeType](elem).asRight
        case elem: Float => Coproduct[NumberLikeType](elem).asRight
        case elem: java.sql.Timestamp => Coproduct[NumberLikeType](elem).asRight
        case elem: java.util.Date => Coproduct[NumberLikeType](new java.sql.Timestamp(elem.getTime())).asRight
        case elem: java.sql.Date  => Coproduct[NumberLikeType](new java.sql.Timestamp(elem.getTime())).asRight
        case _ => InvalidType("Range", "min/max", " ").asLeft
      }
    }

    def assertType[B](minimum: NumberLikeType, maximum: NumberLikeType): Either[ValidationError, NumberLikeType] ={
      maximum.asRight
    }

    val minimumCol=NumberLikeConversion(col.groupBy().agg(min(name)).head().get(0))
    val maximumCol=NumberLikeConversion(col.groupBy().agg(max(name)).head().get(0))

    val minimum: Either[ValidationError, Boolean]=minimumCol.flatMap(min => assertType(op.min, min).map{
      result=>op.min.lessThanOrEqual(result)
    })
    val maximum: Either[ValidationError, Boolean]=maximumCol.flatMap(max => assertType(op.max, max).map{
      result =>  op.max.moreThanOrEqual(result)
    })

    minimum.flatMap(right => maximum.map(right2 => right && right2))
  }
}
