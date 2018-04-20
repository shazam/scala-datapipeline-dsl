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

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME

import org.json4s._

object AwsDataPipeline {

  def apply(name: String): PipelineBuilder = PipelineBuilder(name)

}

case class PipelineBuilder(name: String,
                           defaults: Defaults = Defaults(),
                           schedule: Option[Schedule] = None,
                           snsAlarm: Option[SnsAlarm] = None,
                           rootActivities: Seq[Activity] = Nil) {

  def withDefaults(failureAndRerunMode: FailureAndRerunMode = FailureAndRerunMode.None,
                   role: Option[String] = None,
                   resourceRole: Option[String] = None,
                   pipelineLogUri: Option[String] = None): PipelineBuilder =
    this.copy(defaults = Defaults(failureAndRerunMode = failureAndRerunMode, role = role, resourceRole = resourceRole, pipelineLogUri = pipelineLogUri))

  def withSnsAlarm(topicArn: String, subject: String, message: String): PipelineBuilder =
    this.copy(snsAlarm = Some(SnsAlarm(topicArn, subject, message)))

  def withSchedule(frequency: PipelineFrequency, startDateTimeIso: String): PipelineBuilder =
    this.copy(schedule = Some(CronSchedule(frequency = frequency, startTime = LocalDateTime.parse(startDateTimeIso, ISO_LOCAL_DATE_TIME))))

  def withOnDemandSchedule: PipelineBuilder = this.copy(schedule = Some(OnDemandSchedule))

  def withActivities(activity: Activity, activities: Activity*): PipelineBuilder =
    withActivities(Seq(activity) ++ activities)

  def withActivities(activities: Seq[Activity], moreActivities: Seq[Activity]*): PipelineBuilder =
    this.copy(rootActivities = rootActivities ++ activities ++ moreActivities.flatten)

  /**
    * Flattened list of all activities
    */
  lazy val activities: Seq[Activity] = Activity.resolveDependencyTree(rootActivities)

  /**
    * All databases
    */
  lazy val databases: Seq[Database] = {
    activities
      .collect {
        case sqlActivity: SqlActivity => sqlActivity
      }
      .map(_.database)
      .distinct
  }

  /**
    * All preconditions
    */
  lazy val preconditions: Seq[Precondition] = activities.flatMap(_.preconditions).distinct

  /**
    * All resources
    */
  lazy val resources: Seq[Resource] = {
    activities
      .collect {
        case emrActivity: EmrActivity => emrActivity
      }
      .map(_.emrCluster)
      .distinct
  }

  /**
    * Data pipeline rendered as JSON
    */
  lazy val json: JObject = Json.render(this)

}
