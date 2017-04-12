package com.wanghuanming.suffixtree

import org.apache.spark.rdd.RDD

import scala.collection.mutable


class McSuffixTree(terminalSymbol: String = "$", baseHeight: Int = 0) {

  val root = new TreeNode(null)

  def insert(str: String, label: String): Unit = {
    // insert all suffixes
    val S = str + terminalSymbol
    for (s <- S.indices.init) {
      // exclude the terminalSymbol
      insertSuffix(RangeSubString(S, s, S.length, label, s))
    }
  }

  def insertSuffix(origin: RangeSubString): Unit = {
    var iter = root
    var remain = origin

    while (remain.nonEmpty) {
      val ch = remain.head

      if (!iter.hasChild(ch)) {
        iter.addChild(remain)
        remain = remain.drop(remain.length)
      } else {
        val child = iter.children(ch)
        val cp = child.seq.commonPrefix(remain)

        if (cp.length < child.seq.length) {
          // split origin child
          iter.updateChild(ch, splitEdgeAt(child, cp.length))
        } else if (cp.length == remain.length) {
          // remain is the leaf
          child.addTerminal(remain)
        }
        remain = remain.drop(cp.length)
      }
      iter = iter.childNode(ch)
    }
  }

  def suffixes: Iterable[String] = {
    val leaves = new mutable.ArrayBuffer[String]()

    def dfs(r: TreeNode, height: Int): Unit = {
      assert(r == root || r.seq.nonEmpty)
      if (r.children.isEmpty && r.seq != null) {
        val h = if (r.terminals.nonEmpty && r.seq.length > 1) height + 1 else height
        r.terminals.foreach { terminal =>
          leaves += Utils.formatNode(terminal.label, h, terminal.index)
        }
        leaves += Utils.formatNode(r.seq.label, h, r.seq.index)
      } else if (r.children.nonEmpty) {
        assert(r.terminals.isEmpty)
        for ((ch, child) <- r.children) {
          dfs(child, height + 1)
        }
      }
    }

    dfs(root, baseHeight)
    leaves
  }

  /** * 为了方便测试的输出模式 * * @return */
  def suffixesTest: Array[String] = {
    val res = new mutable.ArrayBuffer[String]()
    val buff = new mutable.ArrayBuffer[String]()

    def dfs(r: TreeNode, height: Int): Unit = {
      if (r.children.isEmpty) {
        val str = buff.init.mkString + r.seq
        res += s"${r.seq.label}:$str"
        r.terminals.foreach { terminal =>
          res += s"${terminal.label}:$str"
        }
      } else {
        for ((ch, child) <- r.children) {
          buff += child.seq.mkString
          dfs(child, height + 1)
          buff.reduceToSize(buff.length - 1)
        }
        for (terminal <- r.terminals) {
          res += s"${terminal.label}:${buff.mkString}"
        }
      }
    }

    dfs(root, baseHeight)
    res.toArray.sorted
  }

  def splitSprefix(sprefixes: Iterable[String]): Unit = {
    if (root.children == null || root.children.isEmpty) {
      return
    }
    assert(root.children.size == 1)
    val head = root.children.head._1
    var level1 = root.children.head._2
    val sprefix = level1.seq
    println(s"Spliting prefix,sprefix=$sprefix,level1=${level1.seq}")

    var end = level1.seq.length - 1

    while (end >= 1) {
      val common = level1.seq.substring(0, end)
      if (sprefixes.exists(_.startsWith(common.toString))) {
        level1 = splitEdgeAt(level1, common.length)
        root.updateChild(head, level1)
        end = level1.seq.length - 1
        println(s"Spliting prefix,sprefix=$sprefix,level1=${level1.seq}")
      } else {
        end -= 1
      }
    }
  }

  /**
    * r =seq=> origin
    *
    * r =front=> newNode =back=> origin
    */
  private def splitEdgeAt(origin: TreeNode, length: Int): TreeNode = {
    assert(0 < length && length <= origin.seq.length)
    val front = origin.seq.take(length)
    val back = origin.seq.drop(length)
    val newNode = new TreeNode(front)
    origin.seq = back
    newNode.addChild(origin)
    newNode
  }

}

object McSuffixTree {

  def buildByPrefix(str: String, label: String, terminal: String): Array[McSuffixTree] = {
    val alphabet = Utils.getAlphabet(str)
    alphabet.par.map { prefix =>
      buildTree(Iterable(RangeSubString(str + terminal, label)), prefix.toString, alphabet.map(_.toString))
    }.toArray
  }

  def buildOnSpark(rdd: RDD[RangeSubString]): RDD[McSuffixTree] = {
    // 1. add terminal symbol to source strings
    val sc = rdd.context
    val alphabet = getAlphabet(rdd)
    val terminal = getTerminal()
    val terminalRDD = rdd.map(rs => rs.copy(source = rs.source + terminal, end = rs.end + 1)).repartition(sc.defaultParallelism)

    // 2. vertical partition, get sprefixes
    val treeSize = 100 * 10000
    val prefixes = verticalPartition(alphabet, Iterable(terminal), terminalRDD, treeSize)

    // 3. broadcast source strings
    val stringsBV = sc.broadcast(terminalRDD.collect)
    val sprefixesBV = sc.broadcast(prefixes)

    // 4. compute suffix-tree
    sc.parallelize(prefixes.toSeq).map { sprefix =>
      buildTree(stringsBV.value, sprefix, sprefixesBV.value)
    }
  }

  private def getAlphabet(rdd: RDD[RangeSubString]): Iterable[Char] = {
    rdd.flatMap(_.source.distinct).distinct().collect
  }

  private def getTerminal(): Char = {
    // 数据字符保证在ASCII之内，因此用ASCII之外的符号就可以
    255.toChar
  }

  /**
    * Build general suffix-tree by s-prefix
    *
    * @param strs    all strings, not contains terminal
    * @param sprefix s-prefix
    * @return
    */
  private def buildTree(strs: Iterable[RangeSubString], sprefix: String, sprefixes: Iterable[String]): McSuffixTree = {
    val tree = new McSuffixTree

    for (s <- strs) {
      val str = s.toString.intern()
      for (i <- str.indices.init) {
        if (str.startsWith(sprefix, i)) {
          tree.insertSuffix(RangeSubString(str, i, str.length, s.label, i))
        }
      }
    }
    tree.splitSprefix(sprefixes.filter(_ != sprefix))
    tree
  }

  def verticalPartition(alphabet: Iterable[Char],
                        terminalSymbols: Iterable[Char],
                        rdd: RDD[RangeSubString],
                        batchSize: Int): Iterable[String] = {
    val sc = rdd.context
    sc.setLogLevel("WARN")
    println("=============VertialPartition===============")
    val res = mutable.ArrayBuffer[String]()
    val len2strs = mutable.Map[Int, Map[String, Int]]()
    val allSymbols = alphabet ++ terminalSymbols

    var pending = mutable.ArrayBuffer[String]() ++ alphabet.map(_.toString)
    var cnt = 0
    val maxIteration = 10
    while (pending.nonEmpty && cnt < maxIteration) {
      cnt += 1
      println(s"Iteration $cnt, res=$res, pending=$pending")
      val counts = pending.map { sprefix =>
        val len = sprefix.length
        val sprefixs = len2strs.getOrElseUpdate(len, {
          rdd.flatMap(_.source.sliding(len).map(_ -> 1))
            .reduceByKey(_ + _)
            .collect()
            .toMap
        }).withDefaultValue(0)
        sprefix -> sprefixs(sprefix)
      }
      val (first, last) = counts.partition { case (x, c) => 0 < c && c <= batchSize }

      res ++= first.map(_._1)
      pending.clear()
      pending ++= last
        .filter(_._2 > 0)
        .map(_._1)
        .flatMap { sprefix => allSymbols.map(x => sprefix + x) }
    }
    res ++= pending
    sc.setLogLevel("INFO")

    println(s"============After Vertical Paritition, ${res.length} parts,they are $res")
    res
  }
}

