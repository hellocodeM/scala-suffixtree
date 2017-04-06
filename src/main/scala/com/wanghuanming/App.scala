package com.wanghuanming

import org.apache.spark.SparkContext


/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {
    val sc = new SparkContext()

    val strs = Utils.readFromHDFS(sc, args(0))
    val trees = McSuffixTree.buildOnSpark(sc, strs)
    val suffixes = trees.flatMap(_.suffixes)
    suffixes.saveAsTextFile(args(1))
  }

}
