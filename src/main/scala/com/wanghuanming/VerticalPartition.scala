package com.wanghuanming

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by ming on 16-11-15.
  */
object VerticalPartition {

  type SPrefix = String
  type VirtualTree = Array[SPrefix]
  type VirtualTrees = Array[VirtualTree]

  def partition(s: String, alphabet: Iterable[Char], fm: Int): VirtualTrees = {
    // todo: not complete
    var p = new mutable.ArrayBuffer[SPrefix]
    var p1 = alphabet.map(_.toString).toBuffer

    while (p1.nonEmpty) {

      //      val fpis = s.groupBy(x => x).mapValues(_.length)
      val fpis = p1.map(x => x -> s.grouped(x.length).count(_ == x)).toMap
      val buf = new mutable.ArrayBuffer[SPrefix]
      for (pi <- p1) {
        val fpi = fpis(pi)
        if (fpi == 0) {
        } else if (fpi <= fm) {
          p += pi
        } else {
          buf ++= alphabet.map(pi + _)
        }
      }
      p1 = buf
    }
    // sort P in descending fpi order
    val frequency = p.groupBy(x => x).mapValues(_.length)
    p = p.sortBy(frequency).reverse

    val virtualTrees = new ArrayBuffer[VirtualTree]
    while (p.nonEmpty) {
      val g = new ArrayBuffer[SPrefix] += p.head
      p.remove(0)

      val buf = new mutable.ArrayBuffer[SPrefix]
      for (curr <- p) {
        val fCurr = p.count(_ == curr)
        val fG = p.count(g.toSet)

        if (fCurr + fG <= fm) {
          g += curr
          // remove from p
        } else {
          buf += curr
        }
      }
      p = buf
      virtualTrees += g.toArray
    }
    virtualTrees.toArray
  }

}
