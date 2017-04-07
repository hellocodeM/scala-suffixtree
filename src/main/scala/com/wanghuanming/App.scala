package com.wanghuanming

import org.apache.spark.SparkContext


/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {
    val sc = new SparkContext()

    // TODO: find correct terminal
    val trees = McSuffixTree.buildOnSpark(sc, args(0), 1.toChar.toString)
    val suffixes = trees.flatMap(_.suffixes)
    suffixes.saveAsTextFile(args(1))
    sc.defaultParallelism
  }
}
