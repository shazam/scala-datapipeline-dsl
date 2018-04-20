# Overview

Below is a list of all AWS DataPipeline object types and the level of support from this library.

## General Support Notes

* Object `id`s are inferred from the `name` of the object specified in the dag, thus an object named `"My EMR Cluster"` 
will have the id `"My-EMR-Cluster""`.
* `onFail`, `failureAndRerunMode`, `role`, `resourceRole` and `pipelineLogUri` are only supported in the `Default`
object, meaning there can only be one global definition of these per pipeline.
* `onSuccess` actions are not supported.
* `maxActiveInstances` is not supported.
* The fields `reportProgressTimeout`, `lateAfterTimeout` and `onLateAction` are not currently supported.
* The `parent` parameter is not supported.  `parent` is used to build object hierarchies in AWS datapipeline, but this
is much better achieved via Scala hierarchies and/or Scala factory methods.
* User-defined fields are not supported.

# Supported DataPipeline Objects

## Data Nodes

### DynamoDBDataNode

* [ ] Not yet supported

### MySqlDataNode

* [ ] Not yet supported

### RedshiftDataNode

* [ ] Not yet supported

### S3DataNode

* [ ] Not yet supported

### SqlDataNode

* [ ] Not yet supported


## Activities

### CopyActivity

* [ ] Not yet supported

### EmrActivity

* [x] Supported
* TODO: Support for various fields

### HadoopActivity

* [ ] Not yet supported

### HiveActivity

* [ ] Not yet supported

### HiveCopyActivity

* [ ] Not yet supported

### PigActivity

* [ ] Not yet supported

### RedshiftCopyActivity

* [ ] Not yet supported

### ShellCommandActivity

* [x] Supported
* TODO: Support for `scriptArgument`, `runsOn`

### SqlActivity

* [x] Supported
* TODO: Support for `scriptUri`, `scriptArgument`, `runsOn`


## Resources

### Ec2Resource

* [ ] Not yet supported

### EmrCluster

* [x] Supported

### HttpProxy

* [ ] Not yet supported


## Preconditions

### DynamoDBDataExists

* [ ] Not yet supported

### DynamoDBTableExists

* [ ] Not yet supported

### Exists

* [ ] Not yet supported

### S3KeyExists

* [x] Supported

### S3PrefixNotEmpty

* [x] Supported

### ShellCommandPrecondition

* [x] Supported
* TODO: Support for `scriptArgument` and presumably `workerGroup` and `runsOn` but they are not documented


## Databases

### JdbcDatabase 

* [ ] Not yet supported

### RdsDatabase 

* [ ] Not yet supported

### RedshiftDatabase 

* [x] Supported

## Data Formats

* [ ] Not yet supported


## Actions

### SnsAlarm

* [x] Supported
* Note that currently only one alarm per pipeline is supported

### Terminate

* [ ] Not yet supported


## Schedule

* [x] Supported
* Both `ondemand` and `cron` schedules are supported
* `timeseries` schedules are not supported
* Note that currently only one schedule per pipeline is supported 

## Utilities

### ShellScriptConfig

* [ ] Not yet supported

### EmrConfiguration

* [x] Supported

### Property

* [x] Supported
