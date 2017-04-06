package com.wanghuanming

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class TreeNode(var seq: RangeSubString) {

  lazy val children = new mutable.HashMap[Char, TreeNode]
  lazy val terminals = new mutable.ArrayBuffer[RangeSubString]

  def hasChild(ch: Char) = children.contains(ch)

  def childNode(ch: Char): TreeNode = children(ch)

  def addChild(str: RangeSubString): Unit = {
    assert(!children.contains(str.head))
    children += str.head -> new TreeNode(str)
  }

  def addChild(node: TreeNode): Unit = {
    children += node.seq.head -> node
  }

  def updateChild(ch: Char, node: TreeNode): Unit = {
    children.update(ch, node)
  }

  def addTerminal(str: RangeSubString): Unit = {
    terminals += str
  }

  override def toString: String = {
    s"TreeNode($seq)"
  }
}

class McSuffixTree(terminalSymbol: String = "$") {

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

  /**
    * r =seq=> origin
    *
    * r =front=> newNode =back=> origin
    */
  private def splitEdgeAt(origin: TreeNode, length: Int): TreeNode = {
    val front = origin.seq.take(length)
    val back = origin.seq.drop(length)
    val newNode = new TreeNode(front)
    origin.seq = back
    newNode.addChild(origin)
    newNode
  }

  def suffixes: Iterable[String] = {
    val leaves = new mutable.ArrayBuffer[String]()

    def dfs(r: TreeNode, height: Int): Unit = {
      r.terminals.foreach(terminal =>
        leaves += Utils.formatNode(terminal.label, height, terminal.index)
      )
      if (r.children.isEmpty) {
        leaves += Utils.formatNode(r.seq.label, height, r.seq.index)
      } else {
        for ((ch, child) <- r.children) {
          dfs(child, height + 1)
        }
      }
    }

    dfs(root, 0)
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

    dfs(root, 0)
    res.toArray.sorted
  }
}

object McSuffixTree {

  def buildByPrefix(str: String, label: String): Array[McSuffixTree] = {
    val alphabet = str.distinct
    val terminal = "$"
    val S = str + terminal
    alphabet.par.map { prefix =>
      val tree = new McSuffixTree(terminal)
      for (i <- S.indices) {
        if (S(i) == prefix) {
          tree.insertSuffix(RangeSubString(S, i, S.length, label, i))
        }
      }
      tree
    }.toArray
  }

  def buildOnSpark(sc: SparkContext, strs: Iterable[RangeSubString]): RDD[McSuffixTree] = {
    val alphabet = Utils.getAlphabet(strs)
    val prefixes = alphabet.flatMap(x => alphabet.map(_ -> x)).map(x => x._1.toString + x._2)
    val terminal = Utils.genTerminal(alphabet).toString

    buildOnSpark(sc, strs, terminal, alphabet, prefixes)
  }

  def buildOnSpark(sc: SparkContext,
                   strs: Iterable[RangeSubString],
                   terminal: String,
                   alphabet: String,
                   prefixes: Iterable[String]): RDD[McSuffixTree] = {
    val strsBV = sc.broadcast(strs)

    sc.parallelize(prefixes.toSeq).map { prefix =>
      val tree = new McSuffixTree(terminal)

      for (str <- strsBV.value) {
        val S = str + terminal
        for (i <- S.indices) {
          if (S.startsWith(prefix, i)) {
            tree.insertSuffix(RangeSubString(S, i, S.length, str.label, i))
          }
        }
      }
      tree
    }
  }
}
