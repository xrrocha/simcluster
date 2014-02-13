package org.plenix.util

import java.io.FileWriter
import java.io.PrintWriter

import com.typesafe.scalalogging.slf4j.Logging

object IOUtils extends Logging {
  def outputFile(filename: String, autoFlush: Boolean = true) = new PrintWriter(new FileWriter(filename), autoFlush)
}