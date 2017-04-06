package com.wanghuanming

import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("GST")
    val sc = new SparkContext(conf)

    val strs = Utils.readAllStringFromFile(sc, args(0))
    val trees = McSuffixTree.buildOnSpark(sc, strs)
    val suffixes = trees.flatMap(_.suffixes)
    suffixes.saveAsTextFile(args(1))
  }

}
