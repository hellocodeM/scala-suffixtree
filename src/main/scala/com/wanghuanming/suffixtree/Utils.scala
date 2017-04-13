package com.wanghuanming.suffixtree

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by ming on 16-11-14.
  */
object Utils {

  def readAsRDD(sc: SparkContext, filePath: String): RDD[RangeSubString] = {
    sc.wholeTextFiles(filePath, sc.defaultParallelism * 4)
      .map { case (path, content) =>
        val name = new File(path).getName.intern()
        RangeSubString(content.replace("\n", ""), name)
      }
  }

  def getAlphabet(str: String): String = {
    str.distinct
  }

  def getAlphabet(strs: Iterable[RangeSubString]): String = {
    val chars = mutable.HashSet[Char]()
    for (str <- strs) {
      chars ++= str.mkString
    }
    chars.mkString
  }

  def genTerminal(alphabet: String): Char = {
    (1 to 1000).map(_.toChar).toSet.diff(alphabet.toSet).head
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
