package org.plenix.similarity.string

import com.wcohen.ss.api.StringDistance
import com.wcohen.ss.{ Jaro, MongeElkan, SoftTFIDF, BasicStringWrapperIterator, BasicStringWrapper }
import org.plenix.similarity.SimilarityScorer

/**
 * The adapter between SecondString's `StringDistance` and our `SimilarityScorer` trait
 */
class SecondStringAdapter(scorer: com.wcohen.ss.api.StringDistance) extends SimilarityScorer[String] {
  def score(s1: String, s2: String) = scorer.score(s1, s2)
}

/**
 * Jaro distance as implemented by SecondString
 */
object SecondStringJaro extends SecondStringAdapter(new Jaro)

/**
 * MongeElkan distance as implemented by SecondString
 */
object SecondStringMongeElkan extends SecondStringAdapter(new MongeElkan)

/**
 * The soft TFIDF distance as implemented by SecondString
 */
class SecondStringSoftTFIDF(docs: Iterable[String], distance: StringDistance, minSimilarity: Double) extends SimilarityScorer[String] {
  val scorer = {
    val scorer = new SoftTFIDF(distance, minSimilarity)

    import collection.JavaConversions._
    val iterator = new BasicStringWrapperIterator(docs.iterator.map(new BasicStringWrapper(_)))
    scorer.train(iterator)

    scorer
  }

  def score(s1: String, s2: String) = scorer.score(s1, s2)
}