package org.plenix.util

// TODO Test NGramUtils
object NGramUtils {
  // todo Rephrase NGramUtils functions in monadic style
  def ngrams(string: String, length: Int = 4): Iterable[String] =
    string
      .split(' ')
      .filter(_.length >= length)
      .flatMap(_.sliding(length).filter(_.size == length))
      .distinct

  def ngramPairs(elements: Seq[String], length: Int = 4) =
    elements
      .par
      .zipWithIndex
      .flatMap {
        case (element, index) =>
          ngrams(element, length).map(ngram => (ngram, index))
      }
      .groupBy(_._1)
      .values
      .map(_.map(_._2).toSeq.distinct.seq.sorted)
      .flatMap { seq =>
        for {
          left <- 0 until seq.size
          right <- left + 1 until seq.size
        } yield (seq(left), seq(right))
      }
      .toVector
      .distinct
      .seq
}