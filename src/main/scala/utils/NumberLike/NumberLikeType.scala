package utils.NumberLike

import shapeless.{:+:, CNil}

object NumberLikeType {
  type NumberLikeType = Int :+: Double :+: Float :+: java.sql.Timestamp :+: CNil
}
