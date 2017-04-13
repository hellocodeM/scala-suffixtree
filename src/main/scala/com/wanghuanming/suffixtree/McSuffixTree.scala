package com.wanghuanming.suffixtree

import org.apache.spark.rdd.RDD

import scala.collection.mutable


class McSuffixTree extends Serializable with Iterable[(TreeNode, Int)] {

  val root = new TreeNode(null)

  def insert(str: String, label: String): Unit = {
    // insert all suffixes
    val S = str + "$"
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

  def findPrefix(str: RangeSubString): Option[String] = {
    var iter = root
    val path = new StringBuffer
    var remain = str

    while (remain.nonEmpty && iter.children.nonEmpty) {
      val ch = remain.head

      iter.children.get(ch) match {
        case Some(child) =>
          path.append(child.seq.mkString)
          iter = child
          remain = remain.drop(child.seq.length)
        case None =>
          return None
      }
    }
    if (iter.children.isEmpty) {
      Some(path.toString.intern())
    } else {
      None
    }
  }

  def suffixes: Iterator[String] = {
    this.iterator.filter { case (node, h) => node.children.isEmpty }
      .flatMap { case (r, h) =>
        val height = if (r.terminals.nonEmpty && r.seq.length > 1) h + 1 else h
        val seqs = r.terminals :+ r.seq
        seqs.map(t => Utils.formatNode(t.label, height, t.index))
      }
  }

  /**
    *
    * @return iterator for TreeNode and height
    */
  override def iterator: Iterator[(TreeNode, Int)] = {
    new Iterator[(TreeNode, Int)] {
      val stack = mutable.Stack[(TreeNode, Int)]().pushAll(root.children.values.map(_ -> 1))

      override def hasNext: Boolean = stack.nonEmpty

      override def next(): (TreeNode, Int) = {
        val (node, h) = stack.top
        stack.pop
        stack.pushAll(node.children.values.map(_ -> (h + 1)))
        node -> h
      }
    }
  }

  /** * 为了方便测试的输出模式 * * @return */
  def suffixesTest: Array[String] = {
    val res = new mutable.ArrayBuffer[String]()
    val buff = new mutable.ArrayBuffer[String]()

    def dfs(r: TreeNode, height: Int): Unit = {
      if (r.children.isEmpty) {
        val str = buff.init.mkString + r.seq.mkString
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

    dfs(root, 0)
    res.toArray.sorted
  }

  def splitSprefix(thisSprefix: String, otherSprefixes: Iterable[String]): Unit = {
    if (root.children == null || root.children.isEmpty) {
      return
    }
    val start = System.currentTimeMillis()
    assert(root.children.size == 1)
    val head = thisSprefix.head
    val child = root.childNode(head)
    var level1 = root.childNode(head)
    val sprefix = level1.seq

    var end = level1.seq.length - 1

    while (end >= 1) {
      val common = level1.seq.substring(0, end).mkString
      if (otherSprefixes.exists(_.startsWith(common))) {
        level1 = splitEdgeAt(level1, common.length)
        root.updateChild(head, level1)
        end = level1.seq.length - 1
        //        println(s"Spliting prefix,sprefix=$sprefix,level1=${level1.seq}")
      } else {
        end -= 1
      }
    }
    val duration = System.currentTimeMillis() - start
    //    println(s"Time cost in splitSprefix ${duration}ms")
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

  private val parallelScaleFactor = 4

  def buildLocal(str: String, label: String, terminal: String): Array[McSuffixTree] = {
    val alphabet = Utils.getAlphabet(str)
    alphabet.par.map { prefix =>
      buildTree(Iterable(RangeSubString(str + terminal, label)), prefix.toString, alphabet.map(_.toString))
    }.toArray
  }

  private def buildTree(strs: Iterable[RangeSubString], sprefix: String, sprefixes: Iterable[String]): McSuffixTree = {
    val tree = new McSuffixTree

    val start = System.currentTimeMillis()
    var duration = 0L
    val sprefixRS = RangeSubString(sprefix)
    var cnt = 0

    for (str <- strs) {
      for (i <- 0 until str.length if str(i) == sprefix.head && str.startsWith(sprefixRS, i)) {
        // find all suffixes, which starts with sprefix
        cnt += 1
        val start = System.nanoTime()
        val s = str.copy(start = i, index = i)
        tree.insertSuffix(s)
        duration += System.nanoTime() - start
      }
    }
    val total = System.currentTimeMillis() - start
    tree.splitSprefix(sprefix, sprefixes.filter(_ != sprefix))
    println(s"BuildTree for sprefix ($sprefix) total cost ${total}ms, insert $cnt suffixes, cost ${duration / 1000000}ms")
    tree
  }

  def buildOnSpark(rdd: RDD[RangeSubString]): RDD[McSuffixTree] = {
    // 1. add terminal symbol to source strings
    val sc = rdd.context
    val alphabet = getAlphabet(rdd)
    val terminal = getTerminal()
    val terminalRDD = rdd.map(rs => rs.copy(source = rs.source + terminal, end = rs.end + 1))

    // 2. vertical partition, get sprefixes
    val treeSize = 200 * 10000
    val sprefixes = verticalPartition(alphabet, Iterable(terminal), terminalRDD, treeSize)

    val stringCnt = terminalRDD.count()
    //    if (stringCnt < sc.defaultParallelism) {
    //      println(s"Since num of strings is $stringCnt < ${sc.defaultParallelism}, choose build by scanning")
    //      buildByScan(terminalRDD, sprefixes)
    //    } else {
    //      println(s"Since num of strings is $stringCnt > ${sc.defaultParallelism}, choose build by groupBy")
    buildByGroup(terminalRDD, sprefixes)
    //    }
  }

  private def buildByGroup(rdd: RDD[RangeSubString], sprefixes: Iterable[String]): RDD[McSuffixTree] = {
    val sc = rdd.context
    val slidingLen = sprefixes.map(_.length).max
    val sprefixTree = buildSprefixTree(sprefixes)

    val sprefixTreeBV = sc.broadcast(sprefixTree)
    val label2strBV = sc.broadcast(rdd.map(rs => rs.label -> rs.source).collectAsMap())
    val sprefixesBV = sc.broadcast(sprefixes)

    rdd.repartition(rdd.count.toInt min sc.defaultParallelism * parallelScaleFactor)
      .flatMap { rs =>
        // generate all suffixes, but compressed to substring(0, sprefixLen)
        rs.source.indices.init.map { i =>
          // compress suffix with id
          val sub = rs.substring(i, (i + slidingLen) min rs.length).mkString.intern()
          rs.copy(source = sub, start = 0, end = sub.length, index = i)
        }
      }.groupBy[String]((str: RangeSubString) => sprefixTreeBV.value.findPrefix(str).get, sc.defaultParallelism * parallelScaleFactor)
      .map { case (sprefix, strs) =>
        val tree = new McSuffixTree()
        strs.foreach { str =>
          val origin = label2strBV.value(str.label)
          val actual = str.copy(source = origin, start = str.index, end = origin.length)
          // restore compressed suffix from id
          // build suffix tree from these suffixes
          tree.insertSuffix(actual)
        }
        tree.splitSprefix(sprefix, sprefixesBV.value.filter(_ != sprefix))
        tree
      }
  }

  private def buildSprefixTree(sprefixes: Iterable[String]): McSuffixTree = {
    val tree = new McSuffixTree
    sprefixes.zipWithIndex.foreach { case (sprefix, id) => tree.insertSuffix(RangeSubString(sprefix)) }
    tree
  }

  private def getAlphabet(rdd: RDD[RangeSubString]): Iterable[Char] = {
    rdd.flatMap(_.source.distinct).distinct().collect
  }

  private def getTerminal(): Char = {
    // 数据字符保证在ASCII之内，因此用ASCII之外的符号就可以
    255.toChar
  }

  def verticalPartition(alphabet: Iterable[Char],
                        terminalSymbols: Iterable[Char],
                        rdd: RDD[RangeSubString],
                        batchSize: Int): Iterable[String] = {
    val sc = rdd.context
    val stringCnt = rdd.count().toInt
    val strings = rdd.repartition(stringCnt min sc.defaultParallelism * parallelScaleFactor).cache
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
          strings.flatMap(_.source.sliding(len).map(_ -> 1))
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

  private def buildByScan(rdd: RDD[RangeSubString], sprefixes: Iterable[String]): RDD[McSuffixTree] = {
    val sc = rdd.context
    val stringsBV = sc.broadcast(rdd.collect)
    val groupedSprefixes = sprefixes.grouped(1 max sprefixes.size / (sc.defaultParallelism * 1.5).toInt)
    val sprefixesBV = sc.broadcast(sprefixes)

    println(s"BuildByScan: group sprefixes into ${groupedSprefixes.length} prefix trees")

    sc.parallelize(groupedSprefixes.toSeq).map { sprefixes =>
      val prefixTree = buildSprefixTree(sprefixes)
      buildTree(stringsBV.value, sprefixes, prefixTree, sprefixesBV.value)
    }
  }

  private def buildTree(strs: Iterable[RangeSubString], thisSprefixes: Iterable[String],
                        prefixTree: McSuffixTree, sprefixes: Iterable[String]): McSuffixTree = {
    val tree = new McSuffixTree
    val start = System.currentTimeMillis()
    var duration = 0L
    var cnt = 0

    for (str <- strs) {
      for (i <- 0 until str.length if prefixTree.findPrefix(str.substring(i)).nonEmpty) {
        // find all suffixes, which starts with sprefix
        cnt += 1
        val start = System.nanoTime()
        val s = str.copy(start = i, index = i)
        tree.insertSuffix(s)
        duration += System.nanoTime() - start
      }
    }
    val total = System.currentTimeMillis() - start
    thisSprefixes.foreach(sprefix => tree.splitSprefix(sprefix, sprefixes.filter(_ != sprefix)))
    tree
  }
}

