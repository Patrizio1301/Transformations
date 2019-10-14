package utils.NumberLike

import shapeless.{:+:, CNil, Coproduct, Generic, Inl, Inr, Lazy}
import utils.NumberLike.NumberLike.{Aux, instance}

object GenericInstance {

  implicit val cnilNumericalLike: Aux[CNil, CNil] =
    instance((_, _) => true, (_, _) => true)

  implicit def coproductConsTransform[L, R <: Coproduct, LL, RR <: Coproduct]
  (
    implicit
    lch: Aux[L, LL],
    lch1: Aux[L, RR],
    rch: Aux[R, LL :+: RR],
    rch1: Aux[R, RR]
  ): Aux[L :+: R, LL :+: RR] =
    instance(
      {
        case (Inl(l), Inl(ll)) => lch.lessThenOrEqual(l, ll)
        case (Inl(l), Inr(rr)) => lch1.lessThenOrEqual(l, rr)
        case (Inr(r), b) => rch.lessThenOrEqual(r, b)
        case (Inr(r), Inr(rr)) => rch1.lessThenOrEqual(r, rr)
      }, {
        case (Inl(l), Inl(bl)) => lch.moreThenOrEqual(l, bl)
        case (Inl(l), Inr(br)) => false
        case (Inr(r), Inl(bl)) => false
        case (Inr(r), Inr(br)) => rch1.moreThenOrEqual(r, br)
      }
    )

  implicit def genericTransform[A, B, ARepr, BRepr](
                                                     implicit
                                                     gen: Generic.Aux[A, ARepr],
                                                     gen1: Generic.Aux[B, BRepr],
                                                     cch: Lazy[Aux[ARepr, BRepr]]): Aux[A, B] =
    instance(
      (a, b) => cch.value.lessThenOrEqual(gen.to(a), gen1.to(b)),
      (a, b) => cch.value.moreThenOrEqual(gen.to(a), gen1.to(b))
    )

}
