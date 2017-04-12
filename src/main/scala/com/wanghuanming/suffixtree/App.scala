package com.wanghuanming.suffixtree

import org.apache.spark.SparkContext


/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {
    val sc = new SparkContext()

    val input = args(0)
    val output = args(1)
    val rdd = Utils.readAsRDD(sc, input)

    val trees = McSuffixTree.buildOnSpark(rdd)
    val suffixes = trees.flatMap(_.suffixes)
    suffixes.saveAsTextFile(output)
  }
}
