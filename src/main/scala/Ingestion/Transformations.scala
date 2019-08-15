package Ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig.error.ConfigReaderFailures
import transformation.errors.TransformationError
import cats.implicits._
import transformation.Transform
import transformation.Transform._
import transformation.Transformations._


class Transformations {

  def applyTransformations(dfI: DataFrame,
                           transformations: Either[ConfigReaderFailures, Seq[Transform[_]]])(
                            implicit spark: SparkSession): Either[Any, DataFrame] = {

    transformations.flatMap { transformationSeq =>
      transformationSeq.foldLeft(Right(dfI): Either[TransformationError, DataFrame]) {
        (df, transformation) =>
          df.map { dfnext =>transformation.transform(dfnext)
          }
      }
    }
  }

}
