package com.wanghuanming

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by ming on 16-11-14.
  */
object Utils {

  def readAsIterable(sc: SparkContext, filePath: String): Iterable[RangeSubString] = {
    readAsRDD(sc, filePath).collect
  }

  def readAsRDD(sc: SparkContext, filePath: String): RDD[RangeSubString] = {
    sc.wholeTextFiles(filePath)
      .map { case (path, content) =>
        val name = new File(path).getName
        RangeSubString(content.replace("\n", ""), name)
      }
  }

  def getAlphabet(strs: Iterable[RangeSubString]): String = {
    val chars = mutable.HashSet[Char]()
    for (str <- strs) {
      chars ++= str.toString
    }
    chars.mkString
  }

  def genTerminal(alphabet: String): Char = {
    //TODO
    (1 to 10000).map(_.toChar).toSet.diff(alphabet.toSet).head
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
