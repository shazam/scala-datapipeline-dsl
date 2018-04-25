# AWS DataPipeline DSL for Scala

A Scala domain-specific language and toolkit to help you build and maintain AWS DataPipeline definitions. 

This tool aims to ease the burden of maintaining a large suite of AWS DataPipelines.  At Shazam, we use this tool to
define our data pipelines in Scala code and avoid the boilerplate and maintenance headache of managing 10s or 100s of
JSON pipeline configuration files. 

Benefits:-
- Write and maintain Scala code instead of JSON configuration
- Use the DSL's `>>` syntax to clearly express dependencies between your pipeline's activities
- Share code/configuration between your pipeline definitions
- Never write `dependsOn` or `precondition` again, this library manages all ids and object references for you
- Add your own wrapper around this library to predefine most your most commonly-used data pipeline objects

## Tutorial

Build the compiler using `sbt`:
```
$ sbt assembly
```

Create a "Hello World" AWS Data Pipeline definition Scala file:
```
object HelloWorldPipeline {

  import datapipeline.dsl._

  val pipeline =
    AwsDataPipeline(name = "HelloWorldPipeline")
      .withSchedule(
        frequency = Daily,
        startDateTimeIso = "2018-01-01T00:00:00"
      )
      .withActivities(
        ShellCommandActivity(
          name = "Echo Hello World",
          workerGroup = "my-task-runner",
          Command("echo 'Hello AWS Data Pipeline World!'")
        )
      )

}
```

Use the compiler to produce JSON from our Scala definition:

```
$ java -jar target/scala-2.12/datapipeline-compiler.jar HelloWorldPipeline HelloWorldPipeline.scala
Writing pipeline definition to: ./HelloWorldPipeline.json
``` 

The output JSON file contains your pipeline definition ready to deploy to AWS.

## Supported AWS DataPipeline Objects

For details see [Supported Objects](Supported%20Objects.md).

## License

This tool is licensed under [Apache License 2.0](LICENSE).