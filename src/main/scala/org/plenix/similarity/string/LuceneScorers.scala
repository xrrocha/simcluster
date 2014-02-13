package org.plenix.similarity.string

import org.apache.lucene.search.spell.StringDistance
import org.apache.lucene.search.spell.JaroWinklerDistance
import org.apache.lucene.search.spell.LevensteinDistance
import org.plenix.similarity.SimilarityScorer

/**
 * The adapter between Lucene's `StringDistance` and our `SimilarityScorer` trait
 */
class LuceneAdapter(scorer: StringDistance) extends SimilarityScorer[String] {
  def score(s1: String, s2: String) = scorer.getDistance(s1, s2)
}

/**
 * JaroWinkler as implemented by Lucene
 */
object LuceneJaroWinkler extends LuceneAdapter(new JaroWinklerDistance)

/**
 * Levenstein as implemented by Lucene
 */
object LuceneLevenstein extends LuceneAdapter(new LevensteinDistance)