package transformation.transformations.column

import java.sql.Date

import cats.data.Validated.{Invalid, Valid}
import transformation.errors.{InvalidValue, TransformationError}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import cats.implicits._
import utils.conversion.ConversionTypeUtil
import transformation.transformations.Mask
import transformation._

/** Create masked column with default value.
  */
object MaskImp extends Parameters with ConversionTypeUtil {
  import transformation.Transform._
  object MaskInstance extends MaskInstance

  trait MaskInstance {
    implicit val MaskTransformation: Transform[Mask] =
      instance((op: Mask, col: Column) => transformation(op, col))
  }

  private def transformation(op: Mask, col: Column): Either[TransformationError, Column] = {
    parametersForMaskColumn(op).map {
      case (value, typeValue) =>
        logger.info("Masterization: Mask column : {} with value: {} of type: {}",
                    op.field,
                    value.toString,
                    typeValue.toString)
        lit(value).cast(typeValue)
    }
  }

  private def parametersForMaskColumn(op: Mask): Either[TransformationError, (Any, DataType)] = {
    val typeColumn: Either[TransformationError, DataType] = typeToCast(op.dataType) match {
      case Valid(_dataType) => _dataType.asRight
      case Invalid(e) =>
        InvalidValue("Literal",
                     "defaultType",
                     op.dataType,
                     e.toList.map(_.toString()).mkString(" ")).asLeft
    }

    typeColumn.map { _typeColumn =>
      _typeColumn match {
        case dataType: DataType if dataType.isInstanceOf[StringType]  => ("X", StringType)
        case dataType: DataType if dataType.isInstanceOf[IntegerType] => (0, IntegerType)
        case dataType: DataType if dataType.isInstanceOf[DoubleType]  => (0D, DoubleType)
        case dataType: DataType if dataType.isInstanceOf[LongType]    => (0L, LongType)
        case dataType: DataType if dataType.isInstanceOf[FloatType]   => (0F, FloatType)
        case dataType: DataType if dataType.isInstanceOf[DateType]    => (new Date(0), DateType)
        case _                                                        => ("XX", StringType)
      }
    }
  }
}
