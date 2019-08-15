package transformation.transformations.row

import org.apache.spark.sql.DataFrame
import transformation.ParamsTransformer
import transformation.transformations.Select
import cats.implicits._
import transformation.Transform._
import transformation.Transform
import transformation.errors.TransformationError


object Select extends ParamsTransformer {
  object SelectInstance extends SelectInstance

  trait SelectInstance {
    implicit val SelectTransformation: Transform[Select] = instance(
      (op: Select, df: DataFrame) => transformation(op, df)
    )
  }

  def transformation(op: Select, df: DataFrame): Either[TransformationError, DataFrame] = {
    logger.info(s"SelectColumns: selected columns: ${op.columnsToSelect.mkString(", ")}")
    logger.debug(s"SelectColumns: original columns: ${df.columns.mkString(", ")}")
    val _columnsToSelect = op.columnsToSelect.map(_.toColumnName)
    df.select(_columnsToSelect.map(df(_)): _*).asRight
  }
}
