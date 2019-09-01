package transformation.transformations

import com.typesafe.config.Config
import pureconfig.generic.{FieldCoproductHint, ProductHint}
import pureconfig._
import pureconfig.error.ConfigReaderFailures
import transformation.ParamValidator

sealed trait Transformation

final case class Base64(
    field: String,
    encrypted: Option[Boolean] = None
) extends ParamValidator[Base64]
    with ColumnTransformation
    with Transformation {
  def validate: Either[String, Base64] = ???
}

final case class SelectColumns(
    columnsToSelect: Seq[String]
) extends ParamValidator[SelectColumns]
    with Transformation {
  def validate: Either[String, SelectColumns] = ???
}


object TransformationUtils extends TransformationUtils

class TransformationUtils {

  def getTransformation[T<:Transformation](config: Config): Either[ConfigReaderFailures, Transformation] = {
    implicit val hint = ProductHint[Transformation](useDefaultArgs = true)
    implicit val hinte = ProductHint[Transformation](allowUnknownKeys = false)
    implicit def coproductHint[T] = new FieldCoproductHint[T]("type") {
      override def fieldValue(name: String): String = name.toLowerCase
    }
    implicit def hinut[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
    import pureconfig.generic.auto._
    loadConfig[Transformation](config)
  }

}
