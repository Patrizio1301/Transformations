package transformation.transformations

import pureconfig._
import transformation.ParamValidator
import com.typesafe.config.Config
import pureconfig.generic.{FieldCoproductHint, ProductHint}
import cats.implicits._
import shapeless.Generic
import pureconfig._
import pureconfig.error.ConfigReaderFailures

sealed trait Transformation

final case class Base64(
    field: String,
    encrypted: Option[Boolean] = None
) extends ParamValidator[Base64]
    with ColumnTransformation
    with Transformation {
  def validate: Either[String, Base64] = ???
}

final case class Select(
    columnsToSelect: Seq[String]
) extends ParamValidator[Select]
    with Transformation {
  def validate: Either[String, Select] = ???
}


object TransformationUtils extends TransformationUtils

class TransformationUtils {

  class TransformationInput[+A](value: A)

  val generic = Generic[Transformation]


//  def getType[T<:Transformation](config: ConfigReader.Result[TransformationInput[T]]): Any= {
//    config.flatMap { transform => transform match {
//          case t: Base64 => t
//          case t: Select => t
//        }
//    }
//  }

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
