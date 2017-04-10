package hu.sztaki.sm.generator

import java.io.{File, FileOutputStream, PrintWriter}

import breeze.stats.distributions

object Pareto {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new FileOutputStream(
      new File("C:\\Users\\Ehnalis\\Downloads\\pareto.txt"),
      true
    ))

    val pareto = distributions.Pareto(500, 2.0)

    (1 to (1000 * 1000 * 4)).map {
      _ =>
        writer.append(pareto.draw.toString + "\n")
    }
  }
}
