package transformation.transformations.column

import transformation.{ParamValidator, Parameters, Transform}
import transformation.errors.TransformationError
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import cats.implicits._
import transformation.transformations.CommaDelimiter

object CommaDelimiterImp extends Parameters {
  import transformation.Transform._
  object CommaDelimiterInstance extends CommaDelimiterInstance

  trait CommaDelimiterInstance {
    implicit val CommaDelimiterTransformation: Transform[CommaDelimiter] =
      instance((op: CommaDelimiter, col: Column) => transformation(op, col))
  }

  /**
    * Custom trim transformation.
    *
    * @param col to be transformed by masterization.
    * @return Column transformed, if applies.
    */
  private def transformation(op: CommaDelimiter,
                             col: Column): Either[TransformationError, Column] = {
    val _lengthDecimal: Int       = op.lengthDecimal.setDefault(2, "lengthDecimal")
    val _separatorDecimal: String = op.separatorDecimal.setDefault(".", "separatorDecimal")
    val normalizedNumber =
      when(length(col) < _lengthDecimal + 1, lpad(col, _lengthDecimal + 1, "0")).otherwise(col)
    regexp_replace(normalizedNumber,
                   "^([+-]?)(\\d*?)(\\d{0," + _lengthDecimal + "}+)([+-]?)$",
                   "$1$4$2" + _separatorDecimal + "$3").asRight
  }
}
