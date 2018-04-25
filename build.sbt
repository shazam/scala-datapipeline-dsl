name := "datapipeline-dsl"

version := "0.1"

scalaVersion := "2.12.5"

mainClass in assembly := Some("datapipeline.compiler.AwsDataPipelineCompiler")

assemblyJarName in assembly := "datapipeline-compiler.jar"

libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.12.5"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.0-M2"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
