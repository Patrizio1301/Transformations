package transformation.transformations

import pureconfig._
import transformation.ParamValidator
import com.typesafe.config.Config

sealed trait Transformation

case class Base64(
    field: String,
    encrypted: Option[Boolean] = None
) extends ParamValidator[Base64]
    with ColumnTransformation
    with Transformation {
  def validate: Either[String, Base64] = ???
}

case class Select(
    columnsToSelect: Seq[String]
) extends ParamValidator[Select] {
  def validate: Either[String, Select] = ???
}

object TransformationUtils extends TransformationUtils

class TransformationUtils {

  def getTransformation(config: Config): ConfigReader.Result[Transformation] = {
    import generic.auto._
    loadConfig[Transformation](config)
  }

}
