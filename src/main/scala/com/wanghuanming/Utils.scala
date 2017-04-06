package com.wanghuanming

import java.io.File

import org.apache.spark.SparkContext

import scala.collection.mutable

/**
  * Created by ming on 16-11-14.
  */
object Utils {

  def readFromHDFS(sc: SparkContext, filePath: String): mutable.ArrayBuffer[RangeSubString] = {
    val strs = new mutable.ArrayBuffer[RangeSubString]
    val strsRdd: Array[(String, String)] = sc.wholeTextFiles(filePath)
      .map { case (path, content) => new File(path).getName -> content.replace("\n", "") }
      .collect()

    for (file <- strsRdd)
      strs += RangeSubString(file._2, file._1)

    strs
  }

  def getAlphabet(strs: Iterable[RangeSubString]): String = {
    val chars = mutable.HashSet[Char]()
    for (str <- strs) {
      chars ++= str.toString
    }
    chars.mkString
  }

  def genTerminal(alphabet: String): Char = {
    (1 to 128).map(_.toChar).toSet.diff(alphabet.toSet).head
  }

  def suffixes(strs: String*): Array[String] = {
    strs.zipWithIndex.flatMap { case (str: String, i: Int) =>
      suffixesWithLabel(i.toString, str)
    }.toArray.sorted
  }

  def suffixesWithLabel(label: String, str: String): Iterable[String] = {
    suffixesWithLabelTerminal(label, str, "$")
  }

  def suffixesWithLabelTerminal(label: String, str: String, terminal: String): Iterable[String] = {
    str.tails.filter(_.nonEmpty).map(label + ":" + _ + terminal).toArray.sorted
  }

  def formatNode(source: String, height: Int, index: Int): String = {
    s"$height $source:$index"
  }
}
