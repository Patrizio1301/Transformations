package Ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig.error.ConfigReaderFailures
import transformation.errors.TransformationError
import scala.reflect.runtime.universe._
import cats.implicits._
import transformation.transformations._
import transformation.GenericInstance._
import transformation.transformations.TransformationUtils.getTransformation
import utils.EitherUtils.EitherUtils.sequence
import com.typesafe.config.Config
import transformation.Transform
import transformation.Transform._
import transformation.Transformations._

object Transformations extends Transformations

trait Transformations {

  object CommandHandlerRunner {
    def transform[C](command: C)(df: DataFrame)(
        implicit transform: Transform[C]) =
      transform.transform(command)(df)
  }

  def applyTransformations(
      dfI: DataFrame,
      transformations: Seq[Transformation]
  )(implicit spark: SparkSession): Either[Any, DataFrame] = {
    transformations.foldLeft(Right(dfI): Either[TransformationError, DataFrame]) {
      (df, transformation) =>
        df.flatMap { dfnext =>
          CommandHandlerRunner.transform(transformation)(dfnext)
        }
    }
//    println(reify(Transform[Base64]))
//    println(reify(Transform[SelectColumns]))
//    Transform[Base64]
//    Transform[SelectColumns]
//    null
  }

  import scala.collection.JavaConverters._
  def readTransformations(
      config: Config): Either[ConfigReaderFailures, Seq[Transformation]] =
    sequence(
      config
        .getConfigList("transformations")
        .asScala
        .map(conf => getTransformation(conf)))
}
