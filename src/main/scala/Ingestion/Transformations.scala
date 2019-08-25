package Ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig.error.ConfigReaderFailures
import transformation.errors.TransformationError
import cats.implicits._
import shapeless.Poly1
import transformation.Transform._
import transformation.Transform
import transformation.Transformations._
import transformation.transformations._
import transformation.transformations.TransformationUtils._

object Transformations extends Transformations

trait Transformations {

  import shapeless.{Coproduct, :+:, CNil, Inl, Inr}

  type Producto = Base64 :+: Select :+: CNil

  object result extends Poly1 {
    implicit def base64: this.Case[Base64] {type Result = Base64} = at[Base64](t => t)

    implicit def select: this.Case[Select] {type Result = Select} = at[Select](t => t)
  }

  print("hiola")


  //  def applycation( transformations: Either[ConfigReaderFailures, Seq[Transformation]]) ={
  //    transformations.map{
  //      seq => seq.map(element=>{
  //        val u=Coproduct[Producto](element)
  //        print(u)
  //        u.map(result)
  //      })
  //    }
  //  }

  object CommandHandlerRunner {
    def processCommand[C](command: C)(df: DataFrame)(implicit commandHandler: Transform[C]) =
      commandHandler.transform(command)(df)
  }

  def applyTransformations(
                            dfI: DataFrame,
                            transformations: Either[ConfigReaderFailures, Seq[Transformation]])(
                            implicit spark: SparkSession): Either[Any, DataFrame] = {

    transformations.flatMap { transformationSeq =>
      transformationSeq.foldLeft(
        Right(dfI): Either[TransformationError, DataFrame]) {
        (df, transformation) =>
          df.flatMap { dfnext => CommandHandlerRunner.processCommand(transformation)(dfnext) }
      }
    }
  }
}
