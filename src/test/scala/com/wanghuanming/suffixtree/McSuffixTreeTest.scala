package com.wanghuanming.suffixtree

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
      Array("abab", "ab"),
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
    println(s"result -- expected = ${res diff expected}")
    println(s"expected -- result = ${expected diff res}")
    println(s"sources = ${source.mkString(",")}")
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
    val trees = McSuffixTree.buildByPrefix(str, "1", "$")

    assert(trees.flatMap(_.suffixesTest).sorted === Utils.suffixesWithLabel("1", str))
  }

}

@RunWith(classOf[JUnitRunner])
class ExsetMcSuffixTreeTest extends FunSuite with BeforeAndAfter {

  val conf = new SparkConf().setMaster("local[8]").setAppName("oh")
  var sc: SparkContext = _

  def normalize(input: BufferedSource): String = {
    input.getLines().mkString
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

  def expectedResult(name: String): Seq[String] = {
    Source.fromFile(filePath(name)).getLines.toSeq
  }

  def filePath(name: String): String = {
    val resourceDir = "src/test/resources/exset/"
    resourceDir + name
  }

  before {
    sc = new SparkContext(conf)
  }

  after {
    sc.stop()
  }

  test("onSpark") {
    for (i <- 0 to 3) {
      val inputFile = "ex" + i
      val res = "res" + i
      val rdd = Utils.readAsRDD(sc, filePath(inputFile))
      val strs = rdd.collect()
      val terminal = Utils.genTerminal(Utils.getAlphabet(strs)).toString
      val trees = McSuffixTree.buildOnSpark(sc, rdd, strs, terminal)
      val suffixes = trees.flatMap(_.suffixes).collect()
      assert(suffixes.toSet === expectedResult(res).toSet)
    }
  }

  test("verticalPartition") {
    val alphabet = "abcde$"
    val strs = Iterable(
      RangeSubString("abc$"),
      RangeSubString("abc$"),
      RangeSubString("ab$"),
      RangeSubString("bcd$"),
      RangeSubString("bde$"),
      RangeSubString("b$")
    )

    val res = McSuffixTree.verticalPartition(sc, alphabet, sc.parallelize(strs.toSeq), 2)
    val exp = Set("e", "abc", "cd", "c$", "ab$", "b$", "bd", "bcd", "bc$", "d")
    assert(res.toSet === exp)
  }
}
