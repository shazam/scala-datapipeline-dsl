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

import scala.concurrent.duration.FiniteDuration

sealed trait Activity extends DataPipelineObject with Retryable {

  val preconditions: Seq[Precondition]

  val dependsOn: Seq[Activity] = Nil

  def withDependencies(activities: Seq[Activity]): Activity

  def >>(activity: Activity): Activity = {
    //    println(s"${activity.id} depends on ${this.id}")
    activity.withDependencies(Seq(this) ++ activity.dependsOn)
  }

  def >>(activity: Activity, activities: Activity*): Seq[Activity] =
    (Seq(activity) ++ activities).map(this >> _)
}

object Activity {

  def resolveDependencyTree(rootActivities: Seq[Activity]): Seq[Activity] =
    mergeActivities(flattenActivities(rootActivities).reverse)

  /**
    * Traverses dependent activities via 'dependsOn' relationships and returns a flat sequence of activities
    */
  private def flattenActivities(activities: Seq[Activity],
                        visitedIds: Set[String] = Set.empty): Seq[Activity] = {
    activities.reverse.flatMap { // FIXME double reverse (see also resolveDependencyTree)
      activity =>
        //        println(s"Visiting ${activity.id} => resolved deps ${activity.dependsOn}")
        if (visitedIds.contains(activity.id)) throw CyclicalActivitiesException(activity, visitedIds)
        Seq(activity) ++ flattenActivities(activity.dependsOn, visitedIds ++ Set(activity.id))
    }
  }

  /**
    * Merges any activities that are declared more than once.  Throws DuplicateActivityException if
    * multiple activities share the same id (but differ in their definition).  Preserves order.
    */
  private def mergeActivities(activities: Seq[Activity]): Seq[Activity] = {
    activities.zipWithIndex.groupBy(_._1.id).values.toSeq.map {
      activitiesWithIndex =>
        val (as, idxs) = activitiesWithIndex.unzip

        if (as.map(_.withDependencies(Nil)).distinct.length > 1) throw DuplicateActivityException(id = as.head.id)

        val mergedActivity = as.head.withDependencies(as.flatMap(_.dependsOn).distinct)

        val index = idxs.min
        index -> mergedActivity
    }.sortBy(_._1).unzip._2
  }
}

case class EmrActivity(name: String,
                       emrCluster: EmrCluster,
                       steps: Seq[String],
                       preconditions: Seq[Precondition] = Nil,
                       attemptTimeout: Option[FiniteDuration] = None,
                       maximumRetries: Option[Int] = None,
                       retryDelay: Option[FiniteDuration] = None,
                       override val dependsOn: Seq[Activity] = Nil) extends Activity {

  require(steps.nonEmpty, "EmrActivity must have at least one step")

  override val objectType = "EmrActivity"

  override def withDependencies(activities: Seq[Activity]): Activity = copy(dependsOn = activities)
}

case class SqlActivity(name: String,
                       database: Database,
                       workerGroup: String,
                       script: String,
                       preconditions: Seq[Precondition] = Nil,
                       attemptTimeout: Option[FiniteDuration] = None,
                       maximumRetries: Option[Int] = None,
                       retryDelay: Option[FiniteDuration] = None,
                       override val dependsOn: Seq[Activity] = Nil) extends Activity {

  override val objectType = "SqlActivity"

  override def withDependencies(activities: Seq[Activity]): Activity = copy(dependsOn = activities)
}

case class ShellCommandActivity(name: String,
                                workerGroup: String,
                                commandOrScriptUri: CommandOrScriptUri,
                                stdout: Option[String] = None,
                                stderr: Option[String] = None,
                                preconditions: Seq[Precondition] = Nil,
                                attemptTimeout: Option[FiniteDuration] = None,
                                maximumRetries: Option[Int] = None,
                                retryDelay: Option[FiniteDuration] = None,
                                override val dependsOn: Seq[Activity] = Nil) extends Activity {

  override val objectType = "ShellCommandActivity"

  override def withDependencies(activities: Seq[Activity]): Activity = copy(dependsOn = activities)
}

case class CyclicalActivitiesException(activity: Activity, visitedIds: Set[String]) extends Exception(
  s"Cyclical DAG detected when visiting activity with id: '${activity.id}' with already visited activities: ${visitedIds.mkString("'", "', '", "'")}"
)

case class DuplicateActivityException(id: String) extends Exception(
  s"Duplicate activities detected with id: '$id'"
)
