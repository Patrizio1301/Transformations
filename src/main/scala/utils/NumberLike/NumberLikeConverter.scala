package utils.NumberLike

import shapeless.Coproduct
import utils.NumberLike.NumberLikeType.NumberLikeType

trait NumberLikeConverter[A] {
    def apply(a: A): NumberLikeType
}

object NumberLikeConverter {

  implicit object IntToNumberLike extends NumberLikeConverter[Int] {
    def apply(n: Int): NumberLikeType = Coproduct[NumberLikeType](n)
  }

  implicit object DoubleToNumberLike extends NumberLikeConverter[Double] {
    def apply(n: Double): NumberLikeType = Coproduct[NumberLikeType](n)
  }

  implicit object FloatToNumberLike extends NumberLikeConverter[Float] {
    def apply(n: Float): NumberLikeType = Coproduct[NumberLikeType](n)
  }
}
