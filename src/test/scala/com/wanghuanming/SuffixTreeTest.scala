package com.wanghuanming

import com.wanghuanming.SuffixTree.StringWithTag
import org.scalatest.FunSuite

/**
  * Created by ming on 16-11-13.
  */
class SuffixTreeTest extends FunSuite {

  test("suffix trie ") {
    val strs = Array(
      StringWithTag("abab", "1.txt")
    )
    val tree = SuffixTree.fromStrings(strs)

    val leaves = tree.leavesWithHeight

    leaves.foreach(println)
  }
}
