package org.plenix.similarity

/**
 * A similarity metric specific to a given type `T`
 */
trait SimilarityScorer[T] {
  /**
   * Measure the similarity of two elements of the same type.
   * Similarity is expressed as a `Double` value between 0 and 1.
   * `0` means the two elements are not at all similar, `1` means
   * the two elements are completely alike.
   */
  def score(t1: T, t2: T): Double
}
