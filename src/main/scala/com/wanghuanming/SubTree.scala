package com.wanghuanming

/*
import com.wanghuanming.SubTree.SPrefix

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by ming on 16-11-15.
  */
class SubTree(S: String, sprefix: SPrefix) {

  type LeafInfo = Array[Int]
  type BranchInfo = Array[(Char, Char, Int)]
  type ArrayType[T] = mutable.ArrayBuffer[T]

  var L = new ArrayType[Int] // leaf info
  var B = new ArrayType[(Char, Char, Int)] // branch info
  var I = new ArrayType[Int] // index
  var A = new ArrayType[Int] // avtive area
  var R = new ArrayType[Int] // buff of string
  var P = new ArrayType[Int] // the order of appearance in the string S of the leaves in L


  def location(sprefix: SPrefix): Int = ???

  /*
  def prepare(): Unit = {
    var start = sprefix.length
    while (B.contains(null)) {
      val range = getRangeOfSymbols
      for (i <- 0 to L.length) {
        val idx = I(i)
        if (idx != done) {
          R(idx) = readRange(S, L(idx)+ star,t range)
        }
      }

      for (a <- A) {

      }
    }

  }
  */

  def build(): Unit ={

  }
}

/*
object SubTree {

  type SPrefix = String

  def construct(s: String, sprefix: SPrefix): SuffixTree = {

  }
}
*/
*/
