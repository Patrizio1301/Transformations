package transformation.transformations.column

import transformation.{ParamValidator, Parameters, Transform}
import transformation.errors.TransformationError
import cats.implicits._
import transformation.transformations.ColumnTransformation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}
import transformation.transformations.Integrity

/** Checks key in dictionary. If key is not present, it will use a default value.
  */
object IntegrityImp extends Parameters {
  import transformation.Transform._
  object IntegrityInstance extends IntegrityInstance

  trait IntegrityInstance {
    implicit val IntegrityTransformation: Transform[Integrity] =
      instance((op: Integrity, col: Column) => transformation(op, col))
  }

  /**
    * Check integrity of column
    *
    * @param col to be transformed.
    * @return Column transformed.
    */
  private def transformation(op: Integrity, col: Column): Either[TransformationError, Column] = {
    lazy val catalog: Seq[String] = readIntegrityCatalog(op.path)
    logger.info(s"Integrity: Check integrity for column ${op.field}")
    when(not(col.isin(catalog: _*)), op.default).otherwise(col).asRight
  }

  private def readIntegrityCatalog(path: String): Seq[String] = {
    val spark = SparkSession.getDefaultSession.get

    spark.sparkContext.textFile(path).collect.toSeq
  }

}
