package utils.NumberLike

import shapeless.{:+:, CNil, Coproduct, Generic, Inl, Inr, Lazy}
import java.sql.Timestamp

trait NumberLike[A] {
  type B
  def lessThanOrEqual(a: A, b: B): Boolean
  def moreThanOrEqual(a: A, b: B): Boolean
}

object NumberLike {
  type Aux[A, B0] = NumberLike[A] { type B = B0 }

  def apply[A, B](implicit numberLike: Aux[A, B]): Aux[A, B] =
    numberLike

  def lessThenOrEqual[A, B](a: A)(b: B)(
      implicit numberLike: Aux[A, B]): Boolean =
    numberLike.lessThanOrEqual(a, b)
  def moreThenOrEqual[A, B](a: A)(b: B)(
      implicit numberLike: Aux[A, B]): Boolean =
    numberLike.moreThanOrEqual(a, b)

  def instance[A, B0](
      lTOE: (A, B0) => Boolean,
      mTOE: (A, B0) => Boolean
  ): Aux[A, B0] = new NumberLike[A] {
    type B = B0
    def lessThanOrEqual(a: A, b: B): Boolean = lTOE(a, b)
    def moreThanOrEqual(a: A, b: B): Boolean = mTOE(a, b)
  }

  object ops {
    implicit class NumberLikeOps[A](a: A) {
      def lessThanOrEqual[B](b: B)(implicit numberLike: Aux[A, B]): Boolean =
        numberLike.lessThanOrEqual(a, b)
      def moreThanOrEqual[B](b: B)(implicit numberLike: Aux[A, B]): Boolean =
        numberLike.moreThanOrEqual(a, b)
    }
  }

  implicit val intIntNumberLike: Aux[Int, Int] =
    instance(_ <= _, _ >= _)
  implicit val intFloatNumberLike: Aux[Int, Float] =
    instance(_ <= _, _ >= _)
  implicit val intDoubleNumberLike: Aux[Int, Double] =
    instance(_ <= _, _ >= _)
  implicit val DoubleIntNumberLike: Aux[Double, Int] =
    instance(_ <= _, _ >= _)
  implicit val DoubleFloatNumberLike: Aux[Double, Float] =
    instance(_ <= _, _ >= _)
  implicit val DoubleDoubleNumberLike: Aux[Double, Double] =
    instance(_ <= _, _ >= _)
  implicit val floatIntNumberLike: Aux[Float, Int] =
    instance(_ <= _, _ >= _)
  implicit val floatFloatNumberLike: Aux[Float, Float] =
    instance(_ <= _, _ >= _)
  implicit val floatDoubleNumberLike: Aux[Float, Double] =
    instance(_ <= _, _ >= _)

  implicit val cnilNumberLike: Aux[CNil, CNil] =
    instance((_, _) => true, (_, _) => true)
  implicit val intCnilNumberLike: Aux[Int, CNil] =
    instance((_, _) => true, (_, _) => true)
  implicit val floatCnilNumberLike: Aux[Float, CNil] =
    instance((_, _) => true, (_, _) => true)
  implicit val doubleCnilNumberLike: Aux[Double, CNil] =
    instance((_, _) => true, (_, _) => true)
  implicit val cnilIntNumberLike: Aux[CNil, Int] =
    instance((_, _) => true, (_, _) => true)
  implicit val cnilFloatNumberLike: Aux[CNil, Float] =
    instance((_, _) => true, (_, _) => true)
  implicit val cnilDoubleNumberLike: Aux[CNil, Double] =
    instance((_, _) => true, (_, _) => true)

  implicit val timestampTimestampNumberLike: Aux[Timestamp, Timestamp] =
    instance((a, b) => a.before(b), (a, b) => a.after(b))

  implicit val cnilTimestampNumberLike: Aux[CNil, Timestamp] =
    instance((_, _) => true, (_, _) => true)
  implicit val timestampCNilNumberLike: Aux[Timestamp, CNil] =
    instance((_, _) => true, (_, _) => true)

  implicit def coproductTransform[L, R <: Coproduct, LL, RR <: Coproduct](
      implicit
      lch: Aux[L, LL],
      lch2: Aux[L, RR],
      rch: Aux[R, LL],
      rch2: Aux[R, RR]): Aux[L :+: R, LL :+: RR] =
    instance(
      {
        case (Inl(l), Inl(ll)) => lch.lessThanOrEqual(l, ll)
        case (Inl(l), Inr(rr)) => lch2.lessThanOrEqual(l, rr)
        case (Inr(r), Inl(ll)) => rch.lessThanOrEqual(r, ll)
        case (Inr(r), Inr(rr)) => rch2.lessThanOrEqual(r, rr)
      }, {
        case (Inl(l), Inl(ll)) => lch.moreThanOrEqual(l, ll)
        case (Inl(l), Inr(rr)) => lch2.moreThanOrEqual(l, rr)
        case (Inr(r), Inl(ll)) => rch.moreThanOrEqual(r, ll)
        case (Inr(r), Inr(rr)) => rch2.moreThanOrEqual(r, rr)
      }
    )
  implicit def coproductCNilTransform[L, R <: Coproduct, CNil]
    : Aux[L :+: R, CNil] =
    instance(
      {
        case (Inl(l), _) => true
        case (Inr(r), _) => true
      }, {
        case (Inl(l), _) => true
        case (Inr(r), _) => true
      }
    )

  implicit def cNilCoproductTransform[L, R <: Coproduct, CNil]
    : Aux[CNil, L :+: R] =
    instance(
      {
        case (_, Inl(l)) => true
        case (_, Inr(r)) => true
      }, {
        case (_, Inl(l)) => true
        case (_, Inr(r)) => true
      }
    )

  implicit def genericTransform[A, B, ARepr, BRepr](
      implicit
      gen: Generic.Aux[A, ARepr],
      gen1: Generic.Aux[B, BRepr],
      cch: Lazy[Aux[ARepr, BRepr]]): Aux[A, B] =
    instance(
      (a, b) => cch.value.lessThanOrEqual(gen.to(a), gen1.to(b)),
      (a, b) => cch.value.moreThanOrEqual(gen.to(a), gen1.to(b))
    )
}
