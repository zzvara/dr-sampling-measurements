package hu.sztaki.sm

import java.io.{File, FileOutputStream, PrintWriter}

import scala.io.Source

object Stripper {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new FileOutputStream(
      new File("C:\\Users\\Ehnalis\\Projects\\" +
               "dr-sampling-measurements\\measurements-stripped.txt"),
      true
    ))

    Source.fromFile("C:\\Users\\Ehnalis\\Projects\\" +
      "dr-sampling-measurements\\measurements-1.txt").getLines().filter(_.startsWith("{"))
      .foreach {
        line => writer.append(line + "\n")
    }
  }
}
