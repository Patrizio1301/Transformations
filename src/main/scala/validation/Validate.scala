package validation

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Column, DataFrame}
import validation.errors.ValidationError

trait Validate[A] extends LazyLogging {
  def validate(a: A)(b: DataFrame): Either[ValidationError, Boolean]
}

object Validate {
  def apply[A](implicit validate: Validate[A]): Validate[A] = validate

  def validate[A: Validate](a: A)(
      df: DataFrame): Either[ValidationError, Boolean] =
    Validate[A].validate(a)(df)

  def instance[A](
      columnValidation: (A, Column) => Either[ValidationError, Boolean]
  ): Validate[A] = new Validate[A] {
    def validate(a: A)(df: DataFrame): Either[ValidationError, Boolean] =
      columnValidation(a, df(a.asInstanceOf[ColumnValidation].field))
  }

  def instance[A](
      rowValidation: (A, DataFrame) => Either[ValidationError, Boolean]): Validate[A] =
    new Validate[A] {
      def validate(a: A)(df: DataFrame): Either[ValidationError, Boolean] =
        rowValidation(a, df)
    }

  implicit class ValidateOps[A: Validate](a: A) {
    def transform(df: DataFrame): Either[ValidationError, Boolean] =
      Validate[A].validate(a)(df)
  }
}
