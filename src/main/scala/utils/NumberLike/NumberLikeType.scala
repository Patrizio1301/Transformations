package utils.NumberLike

import shapeless.{:+:, CNil}

object NumberLikeType {
  type NumberLikeType = Int :+: Double :+: Float :+: CNil
}
