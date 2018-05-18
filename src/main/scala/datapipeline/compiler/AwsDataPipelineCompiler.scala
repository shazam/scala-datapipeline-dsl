/*
 * Copyright 2018 Shazam Entertainment Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License
 */

package datapipeline.compiler

import java.io.{File, FileOutputStream, FilenameFilter}
import java.net.URLClassLoader
import java.nio.file.Paths

import datapipeline.dsl.PipelineBuilder

import scala.tools.nsc._

object AwsDataPipelineCompiler extends App {

  import AwsDataPipelineCompilerHelpers._

  if (args.length < 2) fail(
    """Usage: datapipeline-compiler <fqcn> <source> [source...]
      |
      |Where:
      |  - fqcn is the fully-qualified class name of the pipeline definition singleton.  E.g. myorg.DataPipeline
      |  - source is either the Scala source file for your pipeline or a directory containing Scala source files
    """.stripMargin
  )

  val (className :: sourcePaths) = args.toList

  compileSources(sourcePaths)

  val pipelineBuilder: PipelineBuilder = reflectivelyLoadPipelineBuilder(className)

  writePipelineJsonToFile(pipelineBuilder)

}

object AwsDataPipelineCompilerHelpers {

  def compileSources(sourcePaths: List[String]): Unit = {
    val sourceFiles = sourcePaths.map(new File(_)).flatMap {
      case directory if directory.isDirectory => directory.listFiles(ScalaFilenameFilter)
      case file if file.isFile => file :: Nil
      case other => sys.error(s"Unexpected input file/directory: $other")
    }

    val compiler = {
      val settings = new Settings()
      settings.usejavacp.value = true

      val global = new Global(settings)

      new global.Run
    }

    compiler.compile(sourceFiles.map(_.getAbsolutePath))
  }

  def reflectivelyLoadPipelineBuilder(className: String): PipelineBuilder = {
    val classLoader = {
      val environmentalClasspath = Option(System.getenv("CLASSPATH")).toList.flatMap(_.split(":"))
      val classPath = (CurrentWorkingDir :: environmentalClasspath).map(new File(_).toURI.toURL)
      new URLClassLoader(classPath.toArray, this.getClass.getClassLoader)
    }

    val clazz = classLoader.loadClass(className + "$")

    if (!clazz.getDeclaredFields.map(_.getName).contains(PipelineField)) fail(
      s"""Error: The class $className does not have a field named '$PipelineField'.
         |Your pipeline definition singleton should include a field named '$PipelineField' of type datapipeline.dsl.PipelineBuilder,
         |e.g.:
         |
         |object MyDataPipeline {
         |
         |  import datapipeline.dsl._
         |
         |  val $PipelineField = AwsDataPipeline(name = "MyDataPipeline", ...)
         |
         |}
        """.stripMargin
    )

    val pipelineBuilderField = clazz.getDeclaredField(PipelineField)
    pipelineBuilderField.setAccessible(true)

    val obj = clazz.getField("MODULE$").get(null) // retrieve the Scala singleton instance

    val pipelineBuilder = pipelineBuilderField.get(obj).asInstanceOf[PipelineBuilder]

    if (pipelineBuilder == null && clazz.getDeclaredMethods.exists(_.getName == "delayedInit")) fail(
      s"Error: Class $className cannot be loaded because it extends either DelayedInit or App."
    )

    pipelineBuilder
  }

  def writePipelineJsonToFile(pipelineBuilder: PipelineBuilder): Unit = {
    val filename = s"$CurrentWorkingDir${pipelineBuilder.name}.json"

    println(s"Writing pipeline definition to: $filename")

    val os = new FileOutputStream(filename)
    try {
      os.write {
        import org.json4s.native.JsonMethods._

        pretty(render(pipelineBuilder.json)).getBytes("UTF-8")
      }
    } finally {
      os.close()
    }
  }

  def fail(message: String): Unit = {
    System.err.println(message)
    System.exit(1)
  }

  val PipelineField = "pipeline"

  lazy val CurrentWorkingDir = s"${Paths.get("").toAbsolutePath}${File.separator}"

  lazy val ScalaFilenameFilter: FilenameFilter = (_: File, name: String) => name.toLowerCase.endsWith(".scala")

}