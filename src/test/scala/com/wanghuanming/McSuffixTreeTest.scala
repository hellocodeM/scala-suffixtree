package com.wanghuanming

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable
import scala.util.Random

/**
  * Created by ming on 16-11-13.
  */
@RunWith(classOf[JUnitRunner])
class McSuffixTreeTest extends FunSuite {

  test("insertSuffix") {
    val tree = new McSuffixTree
    tree.insertSuffix(RangeSubString("hello", "1.txt"))

    assert(tree.suffixes === Set("hello"))
  }

  test("insertSuffix with prefix") {
    val tree = new McSuffixTree
    tree.insertSuffix(RangeSubString("LLO$1", "1.txt"))
    tree.insertSuffix(RangeSubString("LO$2", "2.txt"))

    val res = tree.suffixes
    assert(res === Set("LLO$1", "LO$2"))
  }

  test("insert") {
    val tree = new McSuffixTree
    tree.insert("hello", "1.txt")

    val expected = Utils.suffixes("hello")
    assert(tree.suffixes === expected)
  }

  test("suffix strings") {
    val tree = new McSuffixTree
    tree.insert("1024", "1")
    tree.insert("24", "2")

    tree.debugDump()
    assert(tree.suffixes === Utils.suffixes("1024", "24"))
  }

  test("two same string") {
    val tree = new McSuffixTree
    tree.insert("hello", "1.txt")
    tree.insert("hello", "2.txt")

    assert(Utils.suffixes("hello", "hello") === tree.suffixes)
  }

  test("some randomly short string") {
    val n = 10
    val size = 10
    val tree = new McSuffixTree
    val strings = new mutable.HashSet[String]

    for (i <- 0 to n) {
      val str = Random.alphanumeric.take(size).mkString
      tree.insert(str, i.toString)
      strings ++= Utils.suffixesWithTerminal(str, "$" + i)
    }

    assert(strings === tree.suffixes)
  }

  test("insert many randomly short string") {
    val n = 10240
    val size = 10
    val tree = new McSuffixTree
    val strings = new mutable.HashSet[String]

    for (i <- 0 to n) {
      val str = Random.alphanumeric.take(size).mkString
      tree.insert(str, i.toString)
      strings ++= Utils.suffixesWithTerminal(str, "$" + i)
    }

    assert(tree.suffixes === strings)
  }

  test("insert some randomly large string") {
    val n = 100
    val size = 1024
    val tree = new McSuffixTree
    val strings = new mutable.HashSet[String]

    for (i <- 0 to n) {
      val str = Random.alphanumeric.take(size).mkString
      tree.insert(str, i.toString)
      strings ++= Utils.suffixesWithTerminal(str, "$" + i)
    }

    assert(tree.suffixes === strings)
  }
}
