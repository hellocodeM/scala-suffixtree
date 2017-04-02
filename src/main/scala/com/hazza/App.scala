package com.hazza

import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("GST")
    val sc = new SparkContext(conf)

    val strs = Utils.readAllStringFromFile(sc, args(0))
    McSuffixTree.buildOnSpark(sc, strs, args(1) + "part-")

  }

}
