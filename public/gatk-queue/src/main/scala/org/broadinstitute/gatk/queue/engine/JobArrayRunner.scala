/*
* Copyright 2012-2015 Broad Institute, Inc.
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

package org.broadinstitute.gatk.queue.engine

import java.io.File

import org.broadinstitute.gatk.queue.function.{QFunction}
import org.broadinstitute.gatk.queue.util.Logging
import org.broadinstitute.gatk.utils.io.IOUtils

/**
 * Trait for job array runners.
 */
trait JobArrayRunner extends Logging {

  var runners = Seq.empty[CommandLineJobRunner]

  // For each command line, CommandLineJobRunner creates a job script file containing the command's invocation.
  // scriptCallsFile contains the paths to these job sctipts, one line for each job in the array
  // If there are {1..n} array jobs, the executing the i-th job consists in simply running the job script from the line i in this script calls file
  var scriptCallsFile: File = _

  def add(runner: CommandLineJobRunner) {
    runners :+= runner
  }

  def createScriptCallsFile(tmpDir: File) = {
    val scriptCalls = new StringBuilder
    runners.foreach(runner => {
      runner.jobScript.setExecutable(true)

      val outputFile = runner.function.jobOutputFile
      val errorFile = if (runner.function.jobErrorFile == null) outputFile else runner.function.jobErrorFile
      scriptCalls.append("%s > %s 2>%s%n".format(runner.jobScript.getPath, outputFile.getPath, errorFile.getPath))
    })
    scriptCallsFile = IOUtils.writeTempFile(scriptCalls.toString(), ".array", "", tmpDir)
    scriptCallsFile
  }

  def deleteScriptCallsFile() {
    if (scriptCallsFile != null)
      IOUtils.tryDelete(scriptCallsFile)
  }
}
