package com.wanghuanming


import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import scala.util.Random

/**
  * Created by ming on 16-11-13.
  */
@RunWith(classOf[JUnitRunner])
class McSuffixTreeTest extends FunSuite {

  test("testInsertSuffix") {
    val cases = Iterable(
      Array("AB$" -> "1", "ABC$" -> "2"),
      Array("ABC$" -> "1", "AB$" -> "2"),
      Array("ABC$" -> "1", "ABC$" -> "2")
    )
    for (c <- cases) {
      val tree = new McSuffixTree()
      for (suffix <- c) {
        tree.insertSuffix(RangeSubString(suffix._1, suffix._2))
      }
      assert(tree.suffixesTest === c.map(x => s"${x._2}:${x._1}"))
    }
  }

  def testInsertForStrings(strings: scala.collection.IndexedSeq[String]): Unit = {
    val tree = new McSuffixTree

    for (i <- strings.indices) {
      tree.insert(strings(i), i.toString)
    }

    assert(tree.suffixesTest === Utils.suffixes(strings: _*))
  }

  test("testInsert") {
    val cases = Iterable(
      Array("1024", "24"),
      Array("1024", "02"),
      Array("100", "10"),
      Array("10", "10"),
      Array("abab"),
      Array("23", "23", "234", "123", "523", "235")
    )
    for (c <- cases) {
      testInsertForStrings(c)
    }
  }

  test("some randomly short string") {
    val n = 10
    val size = 10
    val tree = new McSuffixTree
    val sources = new mutable.ArrayBuffer[String]
    val strings = new mutable.ArrayBuffer[String]

    for (i <- 0 to n) {
      val str = Random.alphanumeric.take(size).mkString
      sources += str
      tree.insert(str, i.toString)
      strings ++= Utils.suffixesWithLabel(i.toString, str)
    }

    val res = tree.suffixesTest
    val expected = strings.sorted
    val diff = (res diff expected) ++ (expected diff res)

    assert(diff.isEmpty, debugDiff(res, expected, diff, sources))
  }

  test("specific case") {
    testInsertForStrings(Array("rM6pXM9Osb", "QZW8xI04sU", "rM6pXM9Osb", "QZW8xI04sU"))
  }

  test("insert many randomly short string") {
    val n = 100
    val size = 10
    val tree = new McSuffixTree
    val sources = new mutable.ArrayBuffer[String]
    val strings = new mutable.ArrayBuffer[String]

    for (i <- 0 to n) {
      val str = Random.alphanumeric.take(size).mkString
      sources += str
      tree.insert(str, i.toString)
      strings ++= Utils.suffixesWithLabel(i.toString, str)
    }

    val res = tree.suffixesTest
    val expected = strings.sorted
    val diff = (res diff expected) ++ (expected diff res)

    assert(diff.isEmpty, debugDiff(res, expected, diff, sources))
  }

  def debugDiff(res: Seq[String], expected: Seq[String], diff: Seq[String], source: Seq[String]): Unit = {
    printf(s"result -- expected = ${res diff expected}\n")
    printf(s"expected -- result = ${expected diff res}\n")
    printf(s"sources = ${source.mkString(",")}\n")
    println("targets: ")
    diff.map(_.split(":")(0).toInt).distinct.foreach { index =>
      println(source(index))
    }
    val suffixes = diff.map(_.split(":")(1).init).toSet
    print(s"sources which contains suffix = ")
    suffixes.foreach { suffix =>
      source.filter(_.contains(suffix)).foreach(println)
    }
  }

  test("insert some randomly large string") {
    val n = 100
    val size = 1024
    val tree = new McSuffixTree
    val strings = new mutable.ArrayBuffer[String]

    for (i <- 0 to n) {
      val str = Random.alphanumeric.take(size).mkString
      tree.insert(str, i.toString)
      strings ++= Utils.suffixesWithLabel(i.toString, str)
    }

    assert(tree.suffixesTest === strings.sorted)
  }

  test("buildByPrefix") {
    val str = "hello"
    val trees = McSuffixTree.buildByPrefix(str, "1")

    assert(trees.flatMap(_.suffixesTest).sorted === Utils.suffixesWithLabel("1", str))
  }
}

@RunWith(classOf[JUnitRunner])
class ExsetMcSuffixTreeTest extends FunSuite {

  def normalize(input: BufferedSource): String = {
    input.getLines().mkString
  }

  def expectedResult(name: String): Seq[String] = {
    Source.fromFile(filePath(name)).getLines.toSeq
  }

  def filePath(name: String): String = {
    val resourceDir = "src/test/resources/exset/"
    resourceDir + name
  }

  test("exset") {
    for (i <- 0 to 3) {
      val inputDir = filePath("ex" + i)
      val res = "res" + i
      val files = new File(inputDir).listFiles()
      val tree = new McSuffixTree

      for (file <- files) {
        val str = normalize(Source.fromFile(file.getAbsolutePath))
        tree.insert(str, file.getName)
      }

      val suffixes = tree.suffixes.toSeq.sorted
      assert(expectedResult(res).sorted === suffixes)
    }
  }
}

@RunWith(classOf[JUnitRunner])
class SparkMcSuffixTreeTest extends FunSuite with BeforeAndAfter {

  val conf = new SparkConf().setMaster("local[4]").setAppName("McSuffixTreeTest")
  var sc: SparkContext = _

  before {
    sc = new SparkContext(conf)
  }

  after {
    sc.stop()
  }

  test("trivial") {
    val str = "hello"
    val strs = Array(RangeSubString(str, "txt1"))
    val alphabet = Utils.getAlphabet(strs)
    val terminal = "$"
    val prefixes = alphabet.map(_.toString)

    val trees = McSuffixTree.buildOnSpark(sc, strs, terminal, alphabet, prefixes)
    val suffixes = trees.flatMap(_.suffixesTest).collect().sorted
    assert(suffixes === Utils.suffixesWithLabel("txt1", str))
  }
}

