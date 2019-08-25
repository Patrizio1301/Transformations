package transformation.transformations

import shapeless.Generic

object TestingShapeless extends TestingShapeless

sealed trait Transformationsu

final case class Base645(
    field: String,
    encrypted: Option[Boolean] = None
) extends Transformationsu

final case class Select5(
    columnsToSelect: Seq[String]
) extends Transformationsu

class TestingShapeless {

  val genu = Generic[Transformationsu]

  val ozuna = genu.to(Base645("HOLA"))

}
