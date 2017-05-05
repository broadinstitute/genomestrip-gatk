/*
* Copyright 2012-2016 Broad Institute, Inc.
* 
* Permission is hereby granted, free of charge, to any person
* obtaining a copy of this software and associated documentation
* files (the "Software"), to deal in the Software without
* restriction, including without limitation the rights to use,
* copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the
* Software is furnished to do so, subject to the following
* conditions:
* 
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
* OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
* NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
* HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
* WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
* THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package org.broadinstitute.gatk.queue.util

import org.broadinstitute.gatk.queue.TryLaterException
import org.broadinstitute.gatk.queue.util.TextFormatUtils._

/**
 * Utilities for retrying a function and waiting a number of minutes.
 */
object Retry extends Logging {
  /**
   * Attempt the function and return the value on success.
   * Each time the function fails the stack trace is logged.
   * @param f Function to run and return its value.
   * @param wait The length in minutes to wait before each attempt.
   * @throws RetryException when all retries are exhausted.
   * @return The successful result of f.
   */
  def attempt[A](f: () => A, wait: Double*): A = {
    var count = 0
    var success = false
    val tries = wait.size + 1
    var result = null.asInstanceOf[A]
    while (!success && count < tries) {
      try {
        result = f()
        success = true
      } catch {
        case e: TryLaterException => {
          throw e
	}
        case e: Exception => {
          count += 1
          if (count < tries) {
            val minutes = wait(count-1)
            logger.error("Caught error during attempt %s of %s."
                    .format(count, tries), e)
            logger.error("Retrying in %.1f minute%s.".format(minutes, plural(minutes)))
            Thread.sleep((minutes * 1000 * 60).toLong)
          } else {
            logger.error("Caught error during attempt %s of %s. Giving up."
                    .format(count, tries), e)
            throw new RetryException("Gave up after %s attempts.".format(tries), e)
          }
        }
      }
    }
    result
  }
}
