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

import java.time.format.DateTimeFormatter

import org.json4s.JsonDSL._
import org.json4s._

object Json {

  def render(pipelineBuilder: PipelineBuilder): JObject = {
    import pipelineBuilder._

    val objects =
      Seq(DefaultJsonRenderer(pipelineBuilder)) ++
        ScheduleJsonRenderer(schedule) ++
        ActivityJsonRenderer(activities) ++
        ResourceJsonRenderer(resources, name) ++
        PreconditionJsonRenderer(preconditions) ++
        DatabaseJsonRenderer(databases) ++
        SnsAlarmRenderer(snsAlarm)

    ("objects" -> objects) ~ ("parameters" -> Seq.empty[JObject])
  }
}

object IdTypeAndNameJsonRenderer {
  def apply(objectType: String, dataPipelineObject: DataPipelineObject): JObject = {
    ("id" -> dataPipelineObject.id) ~
      ("type" -> objectType) ~
      ("name" -> dataPipelineObject.name)
  }
}

object RetryableJsonRenderer {
  def apply(retryable: Retryable): JObject =
    ("attemptTimeout" -> retryable.attemptTimeout.map(_.toString)) ~
      ("maximumRetries" -> retryable.maximumRetries.map(_.toString)) ~
      ("retryDelay" -> retryable.retryDelay.map(_.toString))
}

object DefaultJsonRenderer {
  def apply(pipelineBuilder: PipelineBuilder): JObject = {

    import pipelineBuilder._

    require(schedule.nonEmpty, "A schedule must be provided.")

    ("id" -> "Default") ~
      ("name" -> "Default") ~
      ("role" -> defaults.role) ~
      ("resourceRole" -> defaults.resourceRole) ~
      ("failureAndRerunMode" -> defaults.failureAndRerunMode.mode) ~
      ("pipelineLogUri" -> defaults.pipelineLogUri) ~
      ("scheduleType" -> schedule.map(_.scheduleType)) ~
      ("schedule" -> schedule.filter(_.isInstanceOf[CronSchedule]).map(_ => "ref" -> "Schedule")) ~
      ("onFail" -> snsAlarm.map { alarm => "ref" -> alarm.id })
  }
}

object ScheduleJsonRenderer {
  def apply(maybeSchedule: Option[Schedule]): Option[JObject] =
    maybeSchedule.collect {
      case cron: CronSchedule =>
        ("id" -> "Schedule") ~
          ("type" -> "Schedule") ~
          ("name" -> "Schedule") ~
          ("period" -> cron.frequency.period) ~
          ("startDateTime" -> DateTimeFormatter.ISO_DATE_TIME.format(cron.startTime))
    }
}

object SnsAlarmRenderer {
  def apply(maybeAlarm: Option[SnsAlarm]): Option[JObject] =
    maybeAlarm.map {
      alarm =>
        ("id" -> alarm.id) ~
          ("type" -> "SnsAlarm") ~
          ("name" -> alarm.name) ~
          ("topicArn" -> alarm.topicArn) ~
          ("subject" -> alarm.subject) ~
          ("message" -> alarm.message)
    }
}

object ActivityJsonRenderer {

  def apply(activities: Seq[Activity]): Seq[JObject] = activities.map {
    activity =>
      IdTypeAndNameJsonRenderer(activity.objectType, activity).merge {

        activity match {

          case emrActivity: EmrActivity =>
            import emrActivity._
            ("runsOn" -> ("ref" -> emrCluster.id)) ~
              ("step" -> steps.map(step => step)) ~
              ("precondition" -> preconditions.map(pre => "ref" -> pre.id)) ~
              ("dependsOn" -> dependsOn.map(dep => "ref" -> dep.id))

          case sqlActivity: SqlActivity =>
            import sqlActivity._
            ("database" -> ("ref" -> database.id)) ~
              ("workerGroup" -> workerGroup) ~
              ("script" -> script) ~
              ("precondition" -> preconditions.map(pre => "ref" -> pre.id)) ~
              ("dependsOn" -> dependsOn.map(dep => "ref" -> dep.id))

          case shellCommandActivity: ShellCommandActivity =>
            import shellCommandActivity._
            commandOrScriptUri.json.merge(
              ("workerGroup" -> workerGroup) ~
                ("stdout" -> stdout) ~
                ("stderr" -> stderr) ~
                ("precondition" -> preconditions.map(pre => "ref" -> pre.id)) ~
                ("dependsOn" -> dependsOn.map(dep => "ref" -> dep.id))
            )
        }
      }.merge {
        RetryableJsonRenderer(activity)
      }
  }
}

object PreconditionJsonRenderer {
  def apply(preconditions: Seq[Precondition]): Seq[JObject] = preconditions.map {
    precondition =>
      IdTypeAndNameJsonRenderer(precondition.objectType, precondition).merge {
        precondition match {
          case s3KeyExists: S3KeyExists =>
            import s3KeyExists._
            ("s3Key" -> s3Key) ~
              ("preconditionTimeout" -> preconditionTimeout.map(_.toString))

          case s3PrefixNotEmpty: S3PrefixNotEmpty =>
            import s3PrefixNotEmpty._
            ("s3Prefix" -> s3Prefix) ~
              ("preconditionTimeout" -> preconditionTimeout.map(_.toString))

          case shellCommandPrecondition: ShellCommandPrecondition =>
            import shellCommandPrecondition._

            commandOrScriptUri.json.merge(
              ("stdout" -> stdout) ~
                ("stderr" -> stderr) ~
                ("preconditionTimeout" -> preconditionTimeout.map(_.toString))
            )
        }
      }.merge {
        RetryableJsonRenderer(precondition)
      }
  }
}

object DatabaseJsonRenderer {
  def apply(databases: Seq[Database]): Seq[JObject] = databases.map {
    database =>
      IdTypeAndNameJsonRenderer(database.objectType, database).merge {
        database match {
          case redshiftDatabase: RedshiftDatabase =>
            import redshiftDatabase._

            ("username" -> username) ~
              ("*password" -> password) ~
              ("clusterId" -> clusterId) ~
              ("region" -> region)
        }
      }
  }
}

object ResourceJsonRenderer {
  def apply(resources: Seq[Resource], pipelineName: String): Seq[JObject] = resources.flatMap {
    case emrCluster: EmrCluster =>
      import emrCluster._

      val configurationJson = configuration.toSeq.map {
        conf =>
          ("name" -> conf.name) ~
            ("type" -> "EmrConfiguration") ~
            ("id" -> conf.id) ~
            ("classification" -> conf.classification) ~
            ("property" -> conf.properties.toSeq.indices.map { index => "ref" -> s"${conf.id}-property-$index" })
      }

      val propertiesJson = configuration.toSeq.flatMap(conf => conf.properties.toSeq.map(conf.name -> _)).zipWithIndex.map {
        case ((confName, (key, value)), index) =>
          val propertyName = s"$confName property $index"
          ("id" -> propertyName.replaceAllLiterally(" ", "-")) ~
            ("type" -> "Property") ~
            ("name" -> propertyName) ~
            ("key" -> key) ~
            ("value" -> value)
      }

      val cluster = IdTypeAndNameJsonRenderer(emrCluster.objectType, emrCluster).merge {
        ("enableDebugging" -> enableDebugging.map(_.toString)) ~
          ("releaseLabel" -> releaseLabel) ~
          ("masterInstanceType" -> masterInstanceType) ~
          ("coreInstanceCount" -> coreInstanceCount.map(_.toString)) ~
          ("coreInstanceType" -> coreInstanceType) ~
          ("coreInstanceBidPrice" -> coreInstanceBidPrice.map(_.toString)) ~
          ("useOnDemandOnLastAttempt" -> useOnDemandOnLastAttempt.map(_.toString)) ~
          ("terminateAfter" -> terminateAfter.map(_.toString)) ~
          ("emrLogUri" -> emrLogUri) ~
          ("keyPair" -> keyPair) ~
          ("region" -> region) ~
          ("applications" -> applications) ~
          ("configuration" -> configuration.map { conf => "ref" -> conf.name.replaceAllLiterally(" ", "-") })
      }
      Seq(cluster) ++ configurationJson ++ propertiesJson
  }
}
