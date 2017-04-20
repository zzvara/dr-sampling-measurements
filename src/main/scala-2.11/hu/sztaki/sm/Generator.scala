package hu.sztaki.sm

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.commons.math3.distribution.ZipfDistribution

object Generator {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new FileOutputStream(
      new File("C:\\Users\\Ehnalis\\Data\\zipfian-5.0.txt"),
      true
    ))

    val zipf = new ZipfDistribution(1000 * 1000 * 100, 5)
    (1 to (1000 * 1000 * 4)).map {
      _ =>
        writer.append(zipf.sample().toString + "\n")
    }
  }
}
