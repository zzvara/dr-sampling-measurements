
(1 to 10).foreach { _ =>
  val startTime = System.currentTimeMillis()

  val number = BigDecimal(532.539283)
  val another = BigDecimal(32.491888)

  (1 to 2000000) foreach { _ =>
    number / another
  }

  val endTime = System.currentTimeMillis()

  println(endTime - startTime)
}

(1 to 10).foreach { _ =>
  val startTime = System.currentTimeMillis()

  val number = 532.539283
  val another = 32.491888

  (1 to 2000000) foreach { _ =>
    number / another
  }

  val endTime = System.currentTimeMillis()

  println(endTime - startTime)
}