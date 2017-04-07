package com.wanghuanming

/**
  * Created by ming on 17-4-7.
  */
case class SuffixPartition(prefix: String,
                           strings: Iterable[RangeSubString],
                           height: Int
                          ) {

  def repartition: Iterable[SuffixPartition] = {
    val m = prefix.length
    val n = m + 1
    val first = strings.head
    val p = first.take(n).toString
    val branching = strings.tail.exists(s => s.take(n).toString != p)
    if (!branching) {
      Iterable(SuffixPartition(p, strings, height))
    } else {
      // height++
      strings.map { s => s.take(n).toString -> s }
        .groupBy(_._1)
        .map { case (gp: String, g: Iterable[(String, RangeSubString)]) =>
          SuffixPartition(gp, g.map(_._2), height + 1)
        }
    }
  }
}
