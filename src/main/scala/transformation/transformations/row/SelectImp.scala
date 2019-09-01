package transformation.transformations.row

import org.apache.spark.sql.DataFrame
import transformation.transformations.SelectColumns
import cats.implicits._
import transformation.{ParamsTransformer, Transform}
import transformation.Transform._
import transformation.errors.TransformationError
import transformation.Transform


object SelectImp extends ParamsTransformer {
  object SelectInstance extends SelectInstance

  trait SelectInstance {
    implicit val SelectTransformation: Transform[SelectColumns] = instance(
      (op: SelectColumns, df: DataFrame) => transformation(op, df)
    )
  }

  def transformation(op: SelectColumns, df: DataFrame): Either[TransformationError, DataFrame] = {
    logger.info(s"SelectColumns: selected columns: ${op.columnsToSelect.mkString(", ")}")
    logger.debug(s"SelectColumns: original columns: ${df.columns.mkString(", ")}")
    val _columnsToSelect = op.columnsToSelect.map(_.toColumnName)
    df.select(_columnsToSelect.map(df(_)): _*).asRight
  }
}
