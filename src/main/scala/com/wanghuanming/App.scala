package com.wanghuanming

import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {
    val arg = new Array[String](3)
    arg(0) = "src/test/resources/exset/ex3"
    arg(1) = "src/test/resources/result/"
    arg(2) = "src/test/resources/temp/"
    val conf = new SparkConf().setMaster("local[4]").setAppName("McSuffixTreeTest")
    val sc = new SparkContext(conf)

    val strs = Utils.readAllStringFromFile(sc, arg(0))
    McSuffixTree.buildOnSpark(sc, strs, arg(1) + "part-")

  }

}
