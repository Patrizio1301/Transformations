package transformation

trait ParamValidator[T] {
  def validate: Either[String, T]
}
