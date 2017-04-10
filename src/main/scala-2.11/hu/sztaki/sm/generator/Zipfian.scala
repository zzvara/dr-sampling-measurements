package hu.sztaki.sm.generator

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.commons.math3.distribution.ZipfDistribution

object Zipfian {
  def main(args: Array[String]): Unit = {

    for (
      exponent <- List(1.0, 1.25, 1.5, 2.0, 3.0, 4.0, 5.0);
      cardinality <- List(10, 100, 1000)
    ) {
      val writer = new PrintWriter(new FileOutputStream(
        new File(s"C:\\Users\\Ehnalis\\Downloads\\sampling\\4M\\zipfian-${cardinality}M-$exponent.txt"),
        true
      ))

      val zipf = new ZipfDistribution(1000 * 1000 * cardinality, exponent)
      (1 to (1000 * 1000 * 4)).map {
        _ =>
          writer.append(zipf.sample().toString + "\n")
      }
    }
  }
}
