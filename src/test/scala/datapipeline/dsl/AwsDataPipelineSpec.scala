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

package datapipeline.dsl

import datapipeline.dsl.FailureAndRerunMode.Cascade
import org.json4s._
import org.json4s.native.JsonMethods._
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration.DurationLong

class AwsDataPipelineSpec extends WordSpec with Matchers {

  implicit val formats = DefaultFormats

  "An AWS DataPipeline" can {

    "return pipeline defaults" should {

      "Default" in {
        val pipelineJson =
          basePipeline
            .withDefaults(
              failureAndRerunMode = Cascade,
              role = Some("role"),
              resourceRole = Some("resource-role"),
              pipelineLogUri = Some("s3://log-uri/")
            )
            .withSnsAlarm(topicArn = "", subject = "", message = "")
            .json

        objectWithId(pipelineJson, "Default") shouldBe Some(parse(
          """{
            |  "id": "Default",
            |  "name": "Default",
            |  "role": "role",
            |  "resourceRole": "resource-role",
            |  "failureAndRerunMode": "cascade",
            |  "pipelineLogUri": "s3://log-uri/",
            |  "scheduleType": "cron",
            |  "schedule": {
            |    "ref": "Schedule"
            |  },
            |  "onFail": {
            |    "ref": "SnsAlarm"
            |  }
            |}
          """.stripMargin
        ))
      }
    }

    "return JSON for schedules" should {

      "cron schedule" in {
        val startDateTime = "2018-01-01T00:00:00"
        val cronPipelineJson =
          AwsDataPipeline(name = "pipeline").withSchedule(frequency = Daily, startDateTimeIso = startDateTime).json

        objectsWithType(cronPipelineJson, "Schedule") shouldBe Seq(parse(
          s"""{
             |  "id": "Schedule",
             |  "type": "Schedule",
             |  "name": "Schedule",
             |  "period": "1 day",
             |  "startDateTime": "$startDateTime"
             |}
          """.stripMargin
        ))
      }

      "on-demand schedule" in {
        val onDemandPipelineJson = AwsDataPipeline(name = "pipeline").withOnDemandSchedule.json

        objectsWithType(onDemandPipelineJson, "Schedule") shouldBe empty

        objectWithId(onDemandPipelineJson, "Default").map(_ \ "scheduleType") shouldBe Some(JString("ondemand"))
        objectWithId(onDemandPipelineJson, "Default").map(_ \ "schedule") shouldBe Some(JNothing)
      }
    }

    "return JSON for actions" should {

      "SnsAlarm" in {
        val pipelineJson =
          basePipeline
            .withSnsAlarm(
              topicArn = "arn:topic:arn",
              subject = "It failed!",
              message = "Oh noes"
            )
            .json

        objectWithId(pipelineJson, "SnsAlarm") shouldBe Some(parse(
          """{
            |  "id": "SnsAlarm",
            |  "type": "SnsAlarm",
            |  "name": "SnsAlarm",
            |  "topicArn": "arn:topic:arn",
            |  "subject": "It failed!",
            |  "message": "Oh noes"
            |}
          """.stripMargin
        ))
      }
    }

    "return JSON for activities" should {

      "EmrActivity" in {
        val emrActivity =
          EmrActivity(
            name = "emr activity",
            emrCluster = EmrCluster(
              name = "emr cluster"
            ),
            steps = Seq(
              "step-1",
              "step-2"
            ),
            preconditions = twoPreconditions,
            attemptTimeout = Some(1.hour),
            maximumRetries = Some(2),
            retryDelay = Some(10.minutes)
          )

        val pipelineJson = basePipeline.withActivities(emrActivity).json

        objectWithId(pipelineJson, emrActivity.id) shouldBe Some(parse(
          """{
            |  "id": "emr-activity",
            |  "type": "EmrActivity",
            |  "name": "emr activity",
            |  "runsOn": {
            |    "ref": "emr-cluster"
            |  },
            |  "step": [
            |    "step-1",
            |    "step-2"
            |  ],
            |  "precondition": [
            |    {"ref": "precondition-1"},
            |    {"ref": "precondition-2"}
            |  ],
            |  "attemptTimeout": "1 hour",
            |  "maximumRetries": "2",
            |  "retryDelay": "10 minutes",
            |  "dependsOn": []
            |}
          """.stripMargin
        ))
      }

      "SqlActivity" in {
        val sqlActivity =
          SqlActivity(
            name = "sql activity",
            database = redshiftDatabase(
              name = "redshift database"
            ),
            workerGroup = "worker-group",
            script = "sql script",
            preconditions = twoPreconditions,
            attemptTimeout = Some(1.hour),
            maximumRetries = Some(2),
            retryDelay = Some(10.minutes)
          )

        val pipelineJson = basePipeline.withActivities(sqlActivity).json

        objectWithId(pipelineJson, sqlActivity.id) shouldBe Some(parse(
          """{
            |  "id": "sql-activity",
            |  "type": "SqlActivity",
            |  "name": "sql activity",
            |  "database": {
            |    "ref": "redshift-database"
            |  },
            |  "workerGroup": "worker-group",
            |  "script": "sql script",
            |  "precondition": [
            |    {"ref": "precondition-1"},
            |    {"ref": "precondition-2"}
            |  ],
            |  "attemptTimeout": "1 hour",
            |  "maximumRetries": "2",
            |  "retryDelay": "10 minutes",
            |  "dependsOn": []
            |}
          """.stripMargin
        ))
      }

      "ShellCommandActivity" in {
        val shellCommandActivity = ShellCommandActivity(
          name = "shell command activity",
          workerGroup = "worker-group",
          commandOrScriptUri = Command(command = "echo Hello $1 world", arguments = Seq("datapipeline")),
          stdout = Some("s3://stdout/"),
          stderr = Some("s3://stderr/"),
          preconditions = twoPreconditions,
          attemptTimeout = Some(1.hour),
          maximumRetries = Some(2),
          retryDelay = Some(10.minutes)
        )

        val pipelineJson = basePipeline.withActivities(shellCommandActivity).json

        objectWithId(pipelineJson, shellCommandActivity.id) shouldBe Some(parse(
          """{
            |  "id": "shell-command-activity",
            |  "type": "ShellCommandActivity",
            |  "name": "shell command activity",
            |  "command": "echo Hello $1 world",
            |  "scriptArgument": [ "datapipeline" ],
            |  "workerGroup": "worker-group"
            |  "stdout": "s3://stdout/",
            |  "stderr": "s3://stderr/",
            |  "precondition": [
            |    {"ref": "precondition-1"},
            |    {"ref": "precondition-2"}
            |  ],
            |  "attemptTimeout": "1 hour",
            |  "maximumRetries": "2",
            |  "retryDelay": "10 minutes",
            |  "dependsOn": []
            |}
          """.stripMargin
        ))
      }
    }

    "return JSON for resources" should {

      "EmrCluster" in {

        val emrCluster = EmrCluster(
          name = "emr cluster",
          enableDebugging = Some(true),
          releaseLabel = Some("emr-5.9.0"),
          masterInstanceType = Some("c3.xlarge"),
          coreInstanceCount = Some(6),
          coreInstanceType = Some("c3.xlarge"),
          coreInstanceBidPrice = Some(0.66),
          useOnDemandOnLastAttempt = Some(true),
          terminateAfter = Some(5.hours),
          keyPair = Some("data-engineering"),
          region = Some("us-east-1"),
          emrLogUri = Some("s3://s3-log-bucket"),
          applications = Seq("Spark"),
          configuration = Some(EmrConfiguration(
            name = "optimize spark",
            classification = "spark",
            properties = Map()
          ))
        )

        val pipelineJson = basePipeline
          .withActivities(
            EmrActivity(
              name = "don't care",
              emrCluster = emrCluster,
              steps = Seq("")
            )
          )
          .json

        objectWithId(pipelineJson, emrCluster.id) shouldBe Some(parse(
          """{
            |  "id": "emr-cluster",
            |  "type": "EmrCluster",
            |  "name": "emr cluster",
            |  "enableDebugging": "true",
            |  "releaseLabel": "emr-5.9.0",
            |  "masterInstanceType": "c3.xlarge",
            |  "coreInstanceCount": "6",
            |  "coreInstanceType": "c3.xlarge",
            |  "coreInstanceBidPrice": "0.66",
            |  "useOnDemandOnLastAttempt": "true",
            |  "terminateAfter": "5 hours",
            |  "emrLogUri": "s3://s3-log-bucket",
            |  "keyPair": "data-engineering",
            |  "region": "us-east-1",
            |  "applications": ["Spark"],
            |  "configuration": {
            |    "ref": "optimize-spark"
            |  }
            |
            |}
          """.stripMargin
        ))
      }

      "EmrConfiguration and Properties" in {

        val emrCluster = EmrCluster(
          name = "emr cluster id",
          configuration = Some(EmrConfiguration(
            name = "optimize spark",
            classification = "spark",
            properties = Map(
              "maximizeResourceAllocation" -> "true",
              "reifyMonads" -> "false"
            )
          ))
        )

        val pipelineJson = basePipeline
          .withActivities(
            EmrActivity(
              name = "don't care",
              emrCluster = emrCluster,
              steps = Seq("")
            )
          )
          .json

        objectWithId(pipelineJson, emrCluster.configuration.get.id) shouldBe Some(parse(
          """{
            |  "id": "optimize-spark",
            |  "type": "EmrConfiguration",
            |  "name": "optimize spark",
            |  "classification": "spark",
            |  "property": [
            |    {
            |      "ref": "optimize-spark-property-0"
            |    },
            |    {
            |      "ref": "optimize-spark-property-1"
            |    }
            |  ]
            |}
          """.stripMargin
        ))

        objectWithId(pipelineJson, "optimize-spark-property-0") shouldBe Some(parse(
          """{
            |  "id": "optimize-spark-property-0",
            |  "type": "Property",
            |  "name": "optimize spark property 0",
            |  "key": "maximizeResourceAllocation",
            |  "value": "true"
            |}
          """.stripMargin
        ))

        objectWithId(pipelineJson, "optimize-spark-property-1") shouldBe Some(parse(
          """{
            |  "id": "optimize-spark-property-1",
            |  "type": "Property",
            |  "name": "optimize spark property 1",
            |  "key": "reifyMonads",
            |  "value": "false"
            |}
          """.stripMargin
        ))
      }

    }

    "return JSON for preconditions" should {

      "S3KeyExists" in {
        val s3KeyExists = S3KeyExists(
          name = "s3 key exists",
          s3Key = "s3://key/",
          preconditionTimeout = Some(1.minute),
          attemptTimeout = Some(1.hour),
          maximumRetries = Some(2),
          retryDelay = Some(10.minutes)
        )

        val pipelineJson = basePipeline.withActivities(
          EmrActivity(
            name = "don't-care",
            emrCluster = EmrCluster(
              name = "don't-care"
            ),
            steps = Seq(""),
            preconditions = Seq(
              s3KeyExists
            )
          )
        ).json

        objectWithId(pipelineJson, s3KeyExists.id) shouldBe Some(parse(
          """{
            |  "id": "s3-key-exists",
            |  "type": "S3KeyExists",
            |  "name": "s3 key exists",
            |  "s3Key": "s3://key/",
            |  "preconditionTimeout": "1 minute",
            |  "attemptTimeout": "1 hour",
            |  "maximumRetries": "2",
            |  "retryDelay": "10 minutes"
            |}
          """.stripMargin
        ))
      }

      "S3PrefixNotEmpty" in {
        val s3PrefixNotEmpty = S3PrefixNotEmpty(
          name = "s3 prefix not empty",
          s3Prefix = "s3://prefix/",
          preconditionTimeout = Some(1.minute),
          attemptTimeout = Some(1.hour),
          maximumRetries = Some(2),
          retryDelay = Some(10.minutes)
        )

        val pipelineJson = basePipeline.withActivities(
          EmrActivity(
            name = "don't-care",
            emrCluster = EmrCluster(
              name = "don't-care"
            ),
            steps = Seq(""),
            preconditions = Seq(
              s3PrefixNotEmpty
            )
          )
        ).json

        objectWithId(pipelineJson, s3PrefixNotEmpty.id) shouldBe Some(parse(
          """{
            |  "id": "s3-prefix-not-empty",
            |  "type": "S3PrefixNotEmpty",
            |  "name": "s3 prefix not empty",
            |  "s3Prefix": "s3://prefix/",
            |  "preconditionTimeout": "1 minute",
            |  "attemptTimeout": "1 hour",
            |  "maximumRetries": "2",
            |  "retryDelay": "10 minutes"
            |}
          """.stripMargin
        ))
      }

      "ShellCommandPrecondition" in {
        val shellCommandPrecondition = ShellCommandPrecondition(
          name = "shell command precondition",
          commandOrScriptUri = Command(command = "echo $1", arguments = Seq("hello")),
          stdout = Some("s3://stdout/"),
          stderr = Some("s3://stderr/"),
          preconditionTimeout = Some(1.minute),
          attemptTimeout = Some(1.hour),
          maximumRetries = Some(2),
          retryDelay = Some(10.minutes)
        )

        val pipelineJson = basePipeline.withActivities(
          EmrActivity(
            name = "don't-care",
            emrCluster = EmrCluster(
              name = "don't-care"
            ),
            steps = Seq(""),
            preconditions = Seq(
              shellCommandPrecondition
            )
          )
        ).json

        objectWithId(pipelineJson, shellCommandPrecondition.id) shouldBe Some(parse(
          """{
            |  "id": "shell-command-precondition",
            |  "type": "ShellCommandPrecondition",
            |  "name": "shell command precondition",
            |  "command": "echo $1",
            |  "scriptArgument": [ "hello" ],
            |  "stdout": "s3://stdout/",
            |  "stderr": "s3://stderr/",
            |  "preconditionTimeout": "1 minute",
            |  "attemptTimeout": "1 hour",
            |  "maximumRetries": "2",
            |  "retryDelay": "10 minutes"
            |}
          """.stripMargin
        ))
      }
    }

    "return JSON for databases" should {

      "RedshiftDatabase" in {
        val pipelineJson = basePipeline.withActivities(
          SqlActivity(
            name = "don't care",
            script = "",
            database = RedshiftDatabase(
              name = "redshift database",
              username = "user",
              password = "pass",
              clusterId = "cluster-id",
              region = Some("us-east-1")
            ),
            workerGroup = "worker-group"
          )
        ).json

        objectWithId(pipelineJson, "redshift-database") shouldBe Some(parse(
          """{
            |  "id": "redshift-database",
            |  "type": "RedshiftDatabase",
            |  "name": "redshift database",
            |  "username": "user",
            |  "*password": "pass",
            |  "clusterId": "cluster-id",
            |  "region": "us-east-1"
            |}
          """.stripMargin
        ))
      }

    }

    "express dependencies between activities" should {

      "a simple A >> B dependency" in {
        val pipeline =
          basePipeline
            .withActivities(
              emrActivity("A") >> emrActivity("B")
            )

        val activities = emrActivityObjects(pipeline)

        activities should have length 2
        activities.head.dependsOn shouldBe Some(Nil)
        activities.last.dependsOn shouldBe Some(Seq(DependsOn(ref = "A")))
      }

      "an A >> B >> C dependency chain" in {
        val pipeline =
          basePipeline
            .withActivities(
              emrActivity("A") >> emrActivity("B") >> emrActivity("C")
            )

        val activities = emrActivityObjects(pipeline)

        activities should have length 3
        activities(0).dependsOn shouldBe Some(Nil)
        activities(1).dependsOn shouldBe Some(Seq(DependsOn(ref = "A")))
        activities(2).dependsOn shouldBe Some(Seq(DependsOn(ref = "B")))
      }

      "an A >> (B,C) dependency chain" in {
        val pipeline =
          basePipeline
            .withActivities(
              emrActivity("A") >> (
                emrActivity("B"),
                emrActivity("C")
              )
            )

        val activities = emrActivityObjects(pipeline)

        activities should have length 3
        activities.find(_.id == "A").get.dependsOn shouldBe Some(Nil)
        activities.find(_.id == "B").get.dependsOn shouldBe Some(Seq(DependsOn(ref = "A")))
        activities.find(_.id == "C").get.dependsOn shouldBe Some(Seq(DependsOn(ref = "A")))
      }

      "an (A,B) >> C dependency chain" in {
        val pipeline =
          basePipeline
            .withActivities(
              Seq(
                emrActivity("A"),
                emrActivity("B")
              ) >>
                emrActivity("C")
            )

        val activities = emrActivityObjects(pipeline)

        activities should have length 3
        activities.find(_.id == "A").get.dependsOn shouldBe Some(Nil)
        activities.find(_.id == "B").get.dependsOn shouldBe Some(Nil)
        activities.find(_.id == "C").get.dependsOn shouldBe Some(Seq(DependsOn(ref = "A"), DependsOn(ref = "B")))
      }

      "an (A,B) >> (C,D) dependency chain" in {
        val pipeline =
          basePipeline
            .withActivities(
              Seq(
                emrActivity("A"),
                emrActivity("B")
              ) >>
                (
                  emrActivity("C"),
                  emrActivity("D"),
                )
            )

        val activities = emrActivityObjects(pipeline)

        activities should have length 4
        activities.find(_.id == "A").get.dependsOn shouldBe Some(Nil)
        activities.find(_.id == "B").get.dependsOn shouldBe Some(Nil)
        activities.find(_.id == "C").get.dependsOn shouldBe Some(Seq(DependsOn(ref = "A"), DependsOn(ref = "B")))
        activities.find(_.id == "D").get.dependsOn shouldBe Some(Seq(DependsOn(ref = "A"), DependsOn(ref = "B")))
      }

      "an (A,B) >> (C,D) dependency chain expressed long-hand" in {
        val activityC = emrActivity("C")
        val activityD = emrActivity("D")
        val pipeline =
          basePipeline
            .withActivities(
              emrActivity("A") >> (activityC, activityD),
              emrActivity("B") >> (activityC, activityD)
            )

        val activities = emrActivityObjects(pipeline)

        activities should have length 4
        activities.find(_.id == "A").get.dependsOn shouldBe Some(Nil)
        activities.find(_.id == "B").get.dependsOn shouldBe Some(Nil)
        activities.find(_.id == "C").get.dependsOn shouldBe Some(Seq(DependsOn(ref = "A"), DependsOn(ref = "B")))
        activities.find(_.id == "D").get.dependsOn shouldBe Some(Seq(DependsOn(ref = "A"), DependsOn(ref = "B")))
      }

      "an (A,B)->C dependency where each instance of C is an equal, but different object" in {
        val activityC = emrActivity("C")
        val pipeline =
          basePipeline
            .withActivities(
              emrActivity("A") >> activityC,
              emrActivity("B") >> activityC.copy()
            )

        val activities = emrActivityObjects(pipeline)

        activities should have length 3
        activities.find(_.id == "A").get.dependsOn shouldBe Some(Nil)
        activities.find(_.id == "B").get.dependsOn shouldBe Some(Nil)
        activities.find(_.id == "C").get.dependsOn shouldBe Some(Seq(DependsOn(ref = "A"), DependsOn(ref = "B")))
      }

      "fail on an A -> B -> A cyclical dependency chain" in {
        val activityA = emrActivity("Cycle A")
        val activityB = emrActivity("Cycle B")
        val pipeline =
          basePipeline
            .withActivities(
              activityA >>
                activityB >>
                activityA
            )

        val thrown = the[CyclicalActivitiesException] thrownBy emrActivityObjects(pipeline)

        thrown.getMessage shouldBe "Cyclical DAG detected when visiting activity with id: 'Cycle-A' with already visited activities: 'Cycle-A', 'Cycle-B'"
      }

      "fail if activities share an id" in {
        val pipeline =
          basePipeline
            .withActivities(
              emrActivity("Shared Id"),
              emrActivity("Shared Id", emrClusterId = "alternate-emr-cluster-id")
            )

        val thrown = the[DuplicateActivityException] thrownBy emrActivityObjects(pipeline)

        thrown.getMessage shouldBe "Duplicate activities detected with id: 'Shared-Id'"
      }

      "allow a dependency declared more than once" in {
        val activityA = emrActivity("A")
        val activityB = emrActivity("B")
        val pipeline =
          basePipeline
            .withActivities(
              activityA >> activityB,
              activityA >> activityB
            )

        val activities = emrActivityObjects(pipeline)

        activities should have length 2
        activities.find(_.id == "A").get.dependsOn shouldBe Some(Nil)
        activities.find(_.id == "B").get.dependsOn shouldBe Some(Seq(DependsOn(ref = "A")))
      }

    }

    "convert names to ids" should {

      "basic latin characters and numbers as well as [*.-_] are permitted" in {
        val validId = "ABC-YXZ_abc-xyz_0-9_.*"
        emrActivity(name = validId).id shouldBe validId
      }

      "spaces become dashes" in {
        emrActivity(name = "EMR Activity").id shouldBe "EMR-Activity"
      }

      "non-ascii characters become underscores" in {
        emrActivity(name = "\u0000\u001f\u007f").id shouldBe "___"
      }

    }

  }

  val basePipeline: PipelineBuilder = AwsDataPipeline(name = "base-pipeline")
    .withSchedule(frequency = Daily, startDateTimeIso = "2018-01-01T00:00:00")

  def objectWithId(pipelineJson: JObject, objectId: String): Option[JObject] = {
    (pipelineJson \ "objects").find(_ \ "id" == JString(objectId)).map(_.asInstanceOf[JObject])
  }

  def objectsWithType(pipelineJson: JObject, typeName: String): Seq[JObject] = {
    (pipelineJson \ "objects").filter(_ \ "type" == JString(typeName)).map(_.asInstanceOf[JObject])
  }

  def pipelineObjects(pipelineBuilder: PipelineBuilder): Seq[TestDataPipelineObject] = {
    pipelineBuilder
      .json
      .extract[TestDataPipeline]
      .objects
  }

  def emrActivityObjects(pipelineBuilder: PipelineBuilder): Seq[TestDataPipelineObject] = {
    pipelineObjects(pipelineBuilder).filter(_.`type`.contains("EmrActivity"))
  }

  def emrActivity(name: String, emrClusterId: String = "emr-cluster-id", steps: Seq[String] = Seq("")): EmrActivity =
    EmrActivity(name = name, emrCluster = EmrCluster(name = emrClusterId), steps = steps)

  def redshiftDatabase(name: String): RedshiftDatabase = RedshiftDatabase(name = name, username = "username", password = "password", clusterId = "cluster-id")

  def s3PrefixNotEmpty(name: String): S3PrefixNotEmpty = S3PrefixNotEmpty(name = name, s3Prefix = "s3://prefix/")

  val twoPreconditions = Seq(
    s3PrefixNotEmpty(name = "precondition 1"),
    s3PrefixNotEmpty(name = "precondition 2")
  )

}

case class TestDataPipeline(objects: Seq[TestDataPipelineObject])

case class TestDataPipelineObject(id: String, `type`: Option[String], dependsOn: Option[Seq[DependsOn]])

case class DependsOn(ref: String)
