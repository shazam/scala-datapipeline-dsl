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

import scala.concurrent.duration._

sealed trait Schedule {
  val scheduleType: String
}

object OnDemandSchedule extends Schedule {
  override val scheduleType = "ondemand"
}

case class CronSchedule(frequency: PipelineFrequency, startTime: LocalDateTime) extends Schedule {
  override val scheduleType = "cron"
}

sealed abstract class PipelineFrequency(val period: String)

case object Hourly extends PipelineFrequency(1.hour.toString)

case object Daily extends PipelineFrequency(1.day.toString)

case object Weekly extends PipelineFrequency("1 week")

case object Monthly extends PipelineFrequency("1 month")

case class RunEvery(duration: FiniteDuration) extends PipelineFrequency(duration.toString)
